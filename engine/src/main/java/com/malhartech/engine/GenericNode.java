/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.IdleTimeHandler;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Sink;
import com.malhartech.engine.OperatorStats.PortStats;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.tuple.Tuple;
import java.util.*;
import java.util.Map.Entry;
import org.apache.commons.lang.UnhandledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// inflight changes to the port connections should be captured.
/**
 *
 * The base class for node implementation<p>
 * <br>
 * Implements the base interface {@link com.malhartech.engine.Node}<br>
 * <br>
 * This is the basic functional block of the DAG. It is responsible for the following<br>
 * It emits and consumes tuples<br>
 * Upon window boundary it does house cleaning, state sync up etc<br>
 * Interacts with Stram with a heartbeat protocol<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class GenericNode extends Node<Operator>
{
  protected final HashMap<String, SweepableReservoir> inputs = new HashMap<String, SweepableReservoir>();
  protected ArrayList<DeferredInputConnection> deferredInputConnections = new ArrayList<DeferredInputConnection>();

  // make sure that the cascading logic works here, right now it does not!
  @Override
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
//    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
//      SweepableReservoir original = inputs.get(e.getKey());
//      if (original != null) {
//        inputs.put(e.getKey(), new TappedReservoir(original, e.getValue()));
//      }
//    }
//
//    super.addSinks(sinks);
  }

  @Override
  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
//    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
//      SweepableReservoir someReservoir = inputs.get(e.getKey());
//      if (someReservoir instanceof TappedReservoir) {
//        TappedReservoir sr = (TappedReservoir)someReservoir;
//        assert (sr.stackedSink == e.getValue());
//        inputs.put(e.getKey(), sr.reservoir);
//      }
//    }
//
//    super.removeSinks(sinks);
  }

  public GenericNode(String id, Operator operator)
  {
    super(id, operator);
  }

  @SuppressWarnings("unchecked")
  public InputPort<Object> getInputPort(String port)
  {
    return (InputPort<Object>)descriptor.inputPorts.get(port);
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
//      SweepableReservoir reservoir = inputs.get(port);
//      if (reservoir == null) {
//        int bufferCapacity = attributes == null ? 16 * 1024 : attributes.attrValue(PortContext.BUFFER_SIZE, 16 * 1024);
//        int spinMilliseconds = attributes == null ? 15 : attributes.attrValue(PortContext.SPIN_MILLIS, 15);
//        if (sink instanceof BufferServerSubscriber) {
//          reservoir = new BufferServerReservoir(inputPort.getSink(), port, bufferCapacity, spinMilliseconds, ((BufferServerSubscriber)sink).getSerde(), ((BufferServerSubscriber)sink).getBaseSeconds());
//        }
//        else {
//          reservoir = new InputReservoir(inputPort.getSink(), port, bufferCapacity, spinMilliseconds);
//        }
//      }
    }
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

  /**
   * Originally this method was defined in an attempt to implement the interface Runnable.
   *
   * Although it seems that it's called from another thread which implements Runnable, so we take this
   * opportunity to pass the OperatorContextImpl through the run method. Note that activate does not return as
   * long as there is useful workload for the node.
   */
  @Override
  @SuppressWarnings({"SleepWhileInLoop"})
  public final void run()
  {
    long spinMillis = context.getAttributes().attrValue(OperatorContext.SPIN_MILLIS, 10);
    final boolean handleIdleTime = operator instanceof IdleTimeHandler;
    boolean insideWindow = false;
    int windowCount = 0;
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
                    sinks[s].process(t);
                  }
                  if (windowCount == 0) {
                    insideWindow = true;
                    beginWindowTime = System.nanoTime();
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
                }
                break;

              case END_WINDOW:
                if (t.getWindowId() == currentWindowId) {
                  activePort.remove();

                  if (++receivedEndWindow == totalQueues) {
                    if (++windowCount == applicationWindowCount) {
                      insideWindow = false;
                      operator.endWindow();
                      windowCount = 0;
                    }

                    for (final Sink<Object> output: outputs.values()) {
                      output.process(t);
                    }

                    if (!insideWindow) {
                      endWindowTime = System.nanoTime();
                    }

                    buffers.remove();
                    assert (activeQueues.isEmpty());
                    handleRequests(currentWindowId, !insideWindow);

                    activeQueues.addAll(inputs.values());
                    expectingBeginWindow = activeQueues.size();
                    break activequeue;
                  }
                  else {
                    buffers.remove();
                  }
                }
                else {
                  buffers.remove();
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
                    sinks[s].process(t);
                  }

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
                      descriptor.inputPorts.get(e.getKey()).setConnected(false);
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
                 * Since one of the operators we care about it gone, we should relook at our operators.
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
                  /*
                   * Do the same sequence as the end window since the current window is not ended.
                   */
                  operator.endWindow();
                  insideWindow = false;

                  emitEndWindow();

                  activeQueues.addAll(inputs.values());
                  expectingBeginWindow = activeQueues.size();

                  handleRequests(currentWindowId, true);
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
                    sinks[s].process(tuple);
                  }
                }

                if (break_activequeue) {
                  break activequeue;
                }
                break;

              case CHECKPOINT:
                activePort.remove();
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
          for (SweepableReservoir cb: activeQueues) {
            if (cb.size() > 0) {
              need2sleep = false;
              break;
            }
          }

          if (need2sleep) {
            Thread.sleep(spinMillis);
            if (handleIdleTime) {
              for (SweepableReservoir cb: activeQueues) {
                if (cb.size() > 0) {
                  need2sleep = false;
                  break;
                }
              }

              /*
               * there is still no work scheduled for the operator, so lets give a chance to the operator to handle timeout.
               */
              if (need2sleep) {
                ((IdleTimeHandler)operator).handleIdleTime();
              }
            }
          }
        }
      }
      while (alive);
    }
    catch (InterruptedException ex) {
      alive = false;
    }
    catch (RuntimeException ex) {
      if (ex.getCause() instanceof InterruptedException) {
        alive = false;
      }
      else {
        throw ex;
      }
    }

    if (insideWindow) {
      operator.endWindow();
    }

  }

  @Override
  protected void reportStats(OperatorStats stats, boolean applicationWindowBoundary)
  {
    super.reportStats(stats, applicationWindowBoundary);
    ArrayList<PortStats> ipstats = new ArrayList<PortStats>();
    for (Entry<String, SweepableReservoir> e: inputs.entrySet()) {
      SweepableReservoir ar = e.getValue();
      ipstats.add(new PortStats(e.getKey(), ar.resetCount()));
    }

    stats.inputPorts = ipstats;
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
