/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.UnhandledException;

import com.datatorrent.api.IdleTimeHandler;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Operator.ShutdownException;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.debug.TappedReservoir;
import com.datatorrent.stram.tuple.ResetWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 *
 * The base class for node implementation<p>
 * <br>
 * Implements the base interface {@link com.datatorrent.engine.Node}<br>
 * <br>
 * This is the basic functional block of the DAG. It is responsible for the following<br>
 * It emits and consumes tuples<br>
 * Upon window boundary it does house cleaning, state sync up etc<br>
 * Interacts with Stram with a heartbeat protocol<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class GenericNode extends Node<Operator>
{
  protected final HashMap<String, SweepableReservoir> inputs = new HashMap<String, SweepableReservoir>();
  protected ArrayList<DeferredInputConnection> deferredInputConnections = new ArrayList<DeferredInputConnection>();

  @Override
  @SuppressWarnings("unchecked")
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      SweepableReservoir original = inputs.get(e.getKey());
      if (original instanceof TappedReservoir) {
        TappedReservoir tr = (TappedReservoir)original;
        tr.add(e.getValue());
      }
      else if (original != null) {
        TappedReservoir tr = new TappedReservoir(original, e.getValue());
        inputs.put(e.getKey(), tr);
      }
    }

    super.addSinks(sinks);
  }

  @Override
  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e : sinks.entrySet()) {
      SweepableReservoir reservoir = inputs.get(e.getKey());
      if (reservoir instanceof TappedReservoir) {
        TappedReservoir tr = (TappedReservoir)reservoir;
        tr.remove(e.getValue());
        if (tr.getSinks().length == 0) {
          tr.reservoir.setSink(tr.setSink(null));
          inputs.put(e.getKey(), tr.reservoir);
        }
      }
    }

    super.removeSinks(sinks);
  }

  public GenericNode(Operator operator)
  {
    super(operator);
  }

  @SuppressWarnings("unchecked")
  public InputPort<Object> getInputPort(String port)
  {
    return (InputPort<Object>)descriptor.inputPorts.get(port).component;
  }

  @Override
  public void connectInputPort(String port, final SweepableReservoir reservoir)
  {
    if (reservoir == null) {
      throw new IllegalArgumentException("Reservoir cannot be null for port '" + port + "' on operator '" + operator + "'");
    }

    InputPort<Object> inputPort = getInputPort(port);
    if (inputPort == null) {
      throw new IllegalArgumentException("Port '" + port + "' does not exist on operator '" + operator + "'");
    }

    if (inputs.containsKey(port)) {
      deferredInputConnections.add(new DeferredInputConnection(port, reservoir));
    }
    else {
      inputPort.setConnected(true);
      inputs.put(port, reservoir);
      reservoir.setSink(inputPort.getSink());
    }
  }

  /**
   *
   * @param endWindowTuple the value of endWindowTuple
   */
  protected void processEndWindow(Tuple endWindowTuple)
  {
    endWindowEmitTime = System.currentTimeMillis();

    if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
      insideWindow = false;
      operator.endWindow();
      applicationWindowCount = 0;
    }

    if (endWindowTuple == null) {
      emitEndWindow();
    }
    else {
      for (int s = sinks.length; s-- > 0;) {
        sinks[s].put(endWindowTuple);
      }
      controlTupleCount++;
    }

    if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
      if (checkpoint && checkpoint(currentWindowId)) {
        lastCheckpointedWindowId = currentWindowId;
        checkpoint = false;
      }
      else if (PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE && checkpoint(currentWindowId)) {
        lastCheckpointedWindowId = currentWindowId;
      }
      checkpointWindowCount = 0;
    }

    ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
    reportStats(stats, currentWindowId);
    handleRequests(currentWindowId);
  }

  class ResetTupleTracker
  {
    final ResetWindowTuple tuple;
    SweepableReservoir[] ports;

    ResetTupleTracker(ResetWindowTuple base, int count)
    {
      tuple = base;
      ports = new SweepableReservoir[count];
    }

  }

  boolean insideWindow;
  boolean checkpoint;
  long lastCheckpointedWindowId;

  /**
   * Originally this method was defined in an attempt to implement the interface Runnable.
   *
   * Note that activate does not return as long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings({"SleepWhileInLoop", "UseSpecificCatch", "BroadCatchBlock", "TooBroadCatch"})
  public final void run()
  {
    lastCheckpointedWindowId = 0;
    insideWindow = false;
    checkpoint = false;

    long spinMillis = context.getValue(OperatorContext.SPIN_MILLIS);
    final boolean handleIdleTime = operator instanceof IdleTimeHandler;
    int totalQueues = inputs.size();

    ArrayList<SweepableReservoir> activeQueues = new ArrayList<SweepableReservoir>();
    activeQueues.addAll(inputs.values());

    int expectingBeginWindow = activeQueues.size();
    int receivedEndWindow = 0;
    LinkedList<ResetTupleTracker> resetTupleTracker = new LinkedList<ResetTupleTracker>();

    try {
      do {
        Iterator<SweepableReservoir> buffers = activeQueues.iterator();
        activequeue:
        while (buffers.hasNext()) {
          SweepableReservoir activePort = buffers.next();
          Tuple t = activePort.sweep();
          if (t != null) {
            switch (t.getType()) {
              case BEGIN_WINDOW:
                if (expectingBeginWindow == totalQueues) {
                  activePort.remove();
                  expectingBeginWindow--;
                  currentWindowId = t.getWindowId();
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].put(t);
                  }
                  controlTupleCount++;

                  if (applicationWindowCount == 0) {
                    insideWindow = true;
                    operator.beginWindow(currentWindowId);
                  }
                  receivedEndWindow = 0;
                }
                else if (t.getWindowId() == currentWindowId) {
                  activePort.remove();
                  expectingBeginWindow--;
                }
                else {
                  buffers.remove();

                  /* find the name of the port which got out of sequence tuple */
                  String port = null;
                  for (Entry<String, SweepableReservoir> e : inputs.entrySet()) {
                    if (e.getValue() == activePort) {
                      port = e.getKey();
                    }
                  }

                  assert (port != null); /* we should always find the port */

                  if (PROCESSING_MODE == ProcessingMode.AT_MOST_ONCE) {
                    if (t.getWindowId() < currentWindowId) {
                      /*
                       * we need to fast forward this stream till we find the current
                       * window or the window which is bigger than the current window.
                       */

                      /* lets move the current reservoir in the background */
                      Sink<Object> sink = activePort.setSink(Sink.BLACKHOLE);
                      deferredInputConnections.add(0, new DeferredInputConnection(port, activePort));

                      /* replace it with the reservoir which blocks the tuples in the past */
                      WindowIdActivatedReservoir wiar = new WindowIdActivatedReservoir(port, activePort, currentWindowId);
                      wiar.setSink(sink);
                      inputs.put(port, wiar);
                      activeQueues.add(wiar);
                      break activequeue;
                    }
                    else {
                      expectingBeginWindow--;
                      if (++receivedEndWindow == totalQueues) {
                        processEndWindow(null);
                        activeQueues.addAll(inputs.values());
                        expectingBeginWindow = activeQueues.size();
                        break activequeue;
                      }
                    }
                  }
                  else {
                    logger.error("Catastrophic Error: Out of sequence tuple {} on port {} while expecting {}", new Object[] {Codec.getStringWindowId(t.getWindowId()), port, Codec.getStringWindowId(currentWindowId)});
                    System.exit(2);
                  }
                }
                break;

              case END_WINDOW:
                buffers.remove();
                if (t.getWindowId() == currentWindowId) {
                  activePort.remove();
                  endWindowDequeueTimes.put(activePort, System.currentTimeMillis());
                  if (++receivedEndWindow == totalQueues) {
                    assert (activeQueues.isEmpty());
                    processEndWindow(t);
                    activeQueues.addAll(inputs.values());
                    expectingBeginWindow = activeQueues.size();
                    break activequeue;
                  }
                }
                break;

              case CHECKPOINT:
                activePort.remove();
                long checkPtWinId = t.getWindowId();
                if (lastCheckpointedWindowId < checkPtWinId && !checkpoint) {
                  if (checkpointWindowCount == 0) {
                    if (checkpoint(checkPtWinId)) {
                      lastCheckpointedWindowId = checkPtWinId;
                    }
                  }
                  else {
                    checkpoint = true;
                  }
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].put(t);
                  }
                  controlTupleCount++;
                }
                break;

              case RESET_WINDOW:
                /**
                 * we will receive tuples which are equal to the number of input streams.
                 */
                activePort.remove();
                buffers.remove();

                int baseSeconds = ((ResetWindowTuple)t).getBaseSeconds();
                ResetTupleTracker tracker = null;

                Iterator<ResetTupleTracker> iterator = resetTupleTracker.iterator();
                while (iterator.hasNext()) {
                  tracker = iterator.next();
                  if (tracker.tuple.getBaseSeconds() == baseSeconds) {
                    break;
                  }
                }

                if (tracker == null) {
                  tracker = new ResetTupleTracker((ResetWindowTuple)t, totalQueues);
                  resetTupleTracker.add(tracker);
                }

                int index = 0;
                while (index < tracker.ports.length) {
                  if (tracker.ports[index] == null) {
                    tracker.ports[index++] = activePort;
                    break;
                  }
                  else if (tracker.ports[index] == activePort) {
                    break;
                  }

                  index++;
                }

                if (index == totalQueues) {
                  iterator = resetTupleTracker.iterator();
                  while (iterator.hasNext()) {
                    if (iterator.next().tuple.getBaseSeconds() <= baseSeconds) {
                      iterator.remove();
                    }
                  }
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].put(t);
                  }
                  controlTupleCount++;

                  assert (activeQueues.isEmpty());
                  activeQueues.addAll(inputs.values());
                  expectingBeginWindow = activeQueues.size();
                  break activequeue;
                }
                break;

              case END_STREAM:
                activePort.remove();
                buffers.remove();
                for (Iterator<Entry<String, SweepableReservoir>> it = inputs.entrySet().iterator(); it.hasNext();) {
                  Entry<String, SweepableReservoir> e = it.next();
                  if (e.getValue() == activePort) {
                    if (!descriptor.inputPorts.isEmpty()) {
                      descriptor.inputPorts.get(e.getKey()).component.setConnected(false);
                    }
                    it.remove();

                    /* check the deferred connection list for any new port that should be connected here */
                    Iterator<DeferredInputConnection> dici = deferredInputConnections.iterator();
                    while (dici.hasNext()) {
                      DeferredInputConnection dic = dici.next();
                      if (e.getKey().equals(dic.portname)) {
                        connectInputPort(dic.portname, dic.reservoir);
                        dici.remove();
                        activeQueues.add(dic.reservoir);
                        break activequeue;
                      }
                    }

                    break;
                  }
                }

                /**
                 * We are not going to receive begin window on this ever!
                 */
                expectingBeginWindow--;

                /**
                 * Since one of the operators we care about it gone, we should relook at our ports.
                 * We need to make sure that the END_STREAM comes outside of the window.
                 */
                totalQueues--;

                boolean break_activequeue = false;
                if (totalQueues == 0) {
                  alive = false;
                  break_activequeue = true;
                }
                else if (activeQueues.isEmpty()) {
                  assert (!inputs.isEmpty());
                  processEndWindow(null);
                  activeQueues.addAll(inputs.values());
                  expectingBeginWindow = activeQueues.size();
                  break_activequeue = true;
                }

                /**
                 * also make sure that we update the reset tuple tracker if this stream had delivered any reset tuples.
                 * Check all the reset buffers to see if current input port has already delivered reset tuple. If it has
                 * then we are waiting for something else to deliver the reset tuple, so just clear current reservoir
                 * from the list of tracked reservoirs. If the current input port has not delivered the reset tuple, and
                 * it's the only one which has not, then we consider it delivered and release the reset tuple downstream.
                 */
                ResetWindowTuple tuple = null;
                for (iterator = resetTupleTracker.iterator(); iterator.hasNext();) {
                  tracker = iterator.next();

                  index = 0;
                  while (index < tracker.ports.length) {
                    if (tracker.ports[index] == activePort) {
                      SweepableReservoir[] ports = new SweepableReservoir[totalQueues];
                      System.arraycopy(tracker.ports, 0, ports, 0, index);
                      if (index < totalQueues) {
                        System.arraycopy(tracker.ports, index + 1, ports, index, tracker.ports.length - index - 1);
                      }
                      tracker.ports = ports;
                      break;
                    }
                    else if (tracker.ports[index] == null) {
                      if (index == totalQueues) { /* totalQueues is already adjusted above */
                        if (tuple == null || tuple.getBaseSeconds() < tracker.tuple.getBaseSeconds()) {
                          tuple = tracker.tuple;
                        }

                        iterator.remove();
                      }
                      break;
                    }
                    else {
                      tracker.ports = Arrays.copyOf(tracker.ports, totalQueues);
                    }

                    index++;
                  }
                }

                /*
                 * Since we were waiting for a reset tuple on this stream, we should not any longer.
                 */
                if (tuple != null) {
                  for (int s = sinks.length; s-- > 0;) {
                    sinks[s].put(tuple);
                  }
                  controlTupleCount++;
                }

                if (break_activequeue) {
                  break activequeue;
                }
                break;

              default:
                throw new UnhandledException("Unrecognized Control Tuple", new IllegalArgumentException(t.toString()));
            }
          }
        }

        if (activeQueues.isEmpty() && alive) {
          logger.error("Catastrophic Error: Invalid State - the operator blocked forever!");
          System.exit(2);
        }
        else {
          boolean need2sleep = true;
          for (SweepableReservoir cb : activeQueues) {
            if (cb.size() > 0) {
              need2sleep = false;
              break;
            }
          }

          if (need2sleep) {
            if (handleIdleTime) {
              ((IdleTimeHandler)operator).handleIdleTime();
            }
            else {
              Thread.sleep(spinMillis);
            }
          }
        }
      }
      while (alive);
    }
    catch (ShutdownException se) {
      logger.debug("Shutdown requested by the operator when alive = {}.", alive);
      alive = false;
    }
    catch (Throwable cause) {
      synchronized (this) {
        if (alive) {
          DTThrowable.rethrow(cause);
        }
      }

      Throwable rootCause = cause;
      while (rootCause != null) {
        if (rootCause instanceof InterruptedException) {
          break;
        }
        rootCause = rootCause.getCause();
      }

      if (rootCause == null) {
        DTThrowable.rethrow(cause);
      }
      else {
        logger.debug("Ignoring InterruptedException after shutdown", cause);
      }
    }

    if (insideWindow) {
      endWindowEmitTime = System.currentTimeMillis();
      operator.endWindow();
      if (++applicationWindowCount == APPLICATION_WINDOW_COUNT) {
        applicationWindowCount = 0;
      }

      if (++checkpointWindowCount == CHECKPOINT_WINDOW_COUNT) {
        if (checkpoint || PROCESSING_MODE == ProcessingMode.EXACTLY_ONCE) {
          checkpoint(currentWindowId);
        }
        checkpointWindowCount = 0;
      }

      ContainerStats.OperatorStats stats = new ContainerStats.OperatorStats();
      fixEndWindowDequeueTimesBeforeDeactivate();
      reportStats(stats, currentWindowId);
      handleRequests(currentWindowId);
    }

  }

  /**
   * End window dequeue times may not have been saved for all the input ports during deactivate,
   * so save them for reporting. SPOI-1324.
   */
  private void fixEndWindowDequeueTimesBeforeDeactivate()
  {
    long endWindowDequeueTime = System.currentTimeMillis();
    for (SweepableReservoir sr : inputs.values()) {
      if (endWindowDequeueTimes.get(sr) == null) {
        endWindowDequeueTimes.put(sr, endWindowDequeueTime);
      }
    }
  }

  @Override
  protected void reportStats(ContainerStats.OperatorStats stats, long windowId)
  {
    ArrayList<ContainerStats.OperatorStats.PortStats> ipstats = new ArrayList<ContainerStats.OperatorStats.PortStats>();
    for (Entry<String, SweepableReservoir> e : inputs.entrySet()) {
      SweepableReservoir ar = e.getValue();
      ContainerStats.OperatorStats.PortStats portStats = new ContainerStats.OperatorStats.PortStats(e.getKey());
      portStats.tupleCount = ar.getCount(true);
      portStats.endWindowTimestamp = endWindowDequeueTimes.get(e.getValue());
      ipstats.add(portStats);
    }

    stats.inputPorts = ipstats;
    super.reportStats(stats, windowId);
  }

  protected class DeferredInputConnection
  {
    String portname;
    SweepableReservoir reservoir;

    DeferredInputConnection(String portname, SweepableReservoir reservoir)
    {
      this.portname = portname;
      this.reservoir = reservoir;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(GenericNode.class);
}
