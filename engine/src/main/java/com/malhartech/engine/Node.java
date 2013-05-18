/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.*;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operators.PortMappingDescriptor;
import com.malhartech.debug.MuxSink;
import com.malhartech.tuple.CheckpointTuple;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.tuple.EndWindowTuple;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <OPERATOR>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Node<OPERATOR extends Operator> implements Runnable
{
  /*
   * if the Component is capable of taking only 1 input, call it INPUT.
   * if the Component is capable of providing only 1 output, call it OUTPUT.
   */
  public static final String INPUT = "input";
  public static final String OUTPUT = "output";
  public final int id;
  protected final HashMap<String, Sink<Object>> outputs = new HashMap<String, Sink<Object>>();
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile Sink<Object>[] sinks = Sink.NO_SINKS;
  protected boolean alive;
  protected final OPERATOR operator;
  protected final PortMappingDescriptor descriptor;
  protected long currentWindowId;
  protected int applicationWindowCount;
  protected long stramWindowSize;
  protected long endWindowEmitTime = 0;
  protected long lastSampleCpuTime = 0;
  protected ThreadMXBean tmb;
  protected HashMap<SweepableReservoir, Long> endWindowDequeueTimes = new HashMap<SweepableReservoir, Long>(); // end window dequeue time for input ports
  protected long checkpointedWindowId;

  public Node(int id, OPERATOR operator)
  {
    this.id = id;
    this.operator = operator;

    descriptor = new PortMappingDescriptor();
    Operators.describe(operator, descriptor);
    tmb = ManagementFactory.getThreadMXBean();
  }

  public Operator getOperator()
  {
    return operator;
  }

  public PortMappingDescriptor getPortMappingDescriptor()
  {
    return descriptor;
  }

  public void connectOutputPort(String port, final Sink<Object> sink)
  {
    @SuppressWarnings("unchecked")
    OutputPort<Object> outputPort = (OutputPort<Object>)descriptor.outputPorts.get(port);
    if (outputPort != null) {
      if (sink == null) {
        outputPort.setSink(null);
        outputs.remove(port);
      }
      else {
        outputPort.setSink(sink);
        outputs.put(port, sink);
      }
    }
  }

  public abstract void connectInputPort(String port, final SweepableReservoir reservoir);

  @SuppressWarnings({"unchecked"})
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      OutputPort<?> port = descriptor.outputPorts.get(e.getKey());
      if (port == null) {
        continue;
      }

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == null) {
        port.setSink(e.getValue());
        outputs.put(e.getKey(), e.getValue());
      }
      else if (ics instanceof MuxSink) {
        ((MuxSink)ics).add(e.getValue());
      }
      else {
        MuxSink muxSink = new MuxSink(ics, e.getValue());
        port.setSink(muxSink);
        outputs.put(e.getKey(), muxSink);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
      /* make sure that we ignore all the input ports */
      OutputPort<?> port = descriptor.outputPorts.get(e.getKey());
      if (port == null) {
        continue;
      }

      Sink<Object> ics = outputs.get(e.getKey());
      if (ics == e.getValue()) {
        port.setSink(null);
        outputs.remove(e.getKey());
      }
      else if (ics instanceof MuxSink) {
        MuxSink ms = (MuxSink)ics;
        ms.remove(e.getValue());
        Sink<Object>[] sinks1 = ms.getSinks();
        if (sinks1.length == 0) {
          port.setSink(null);
          outputs.remove(e.getKey());
        }
        else if (sinks1.length == 1) {
          port.setSink(sinks1[0]);
          outputs.put(e.getKey(), sinks1[0]);
        }
      }
    }
  }

  protected OperatorContext context;

  @SuppressWarnings("unchecked")
  public void activate(OperatorContext context)
  {
    boolean activationListener = operator instanceof ActivationListener;

    activateSinks();
    this.alive = true;
    this.context = context;
    this.applicationWindowCount = context.getAttributes().attrValue(OperatorContext.APPLICATION_WINDOW_COUNT, 1);
    this.stramWindowSize = context.getApplicationAttributes().attrValue(DAG.STRAM_WINDOW_SIZE_MILLIS, 500);

    if (activationListener) {
      ((ActivationListener)operator).activate(context);
    }

    run();

    if (activationListener) {
      ((ActivationListener)operator).deactivate();
    }

    this.context = null;
    emitEndStream();
    deactivateSinks();
  }

  public void deactivate()
  {
    alive = false;
  }

  @Override
  public String toString()
  {
    return String.valueOf(id);
  }

  protected void emitEndStream()
  {
//    logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    EndStreamTuple est = new EndStreamTuple(currentWindowId);
    for (final Sink<Object> output: outputs.values()) {
      output.put(est);
    }
  }

  protected void emitEndWindow()
  {
    // This function currently only gets called upon END_STREAM.
    // DO NOT assume this will get called to emit an end window tuple
    EndWindowTuple ewt = new EndWindowTuple(currentWindowId);
    for (final Sink<Object> output: outputs.values()) {
      output.put(ewt);
    }
  }

  public void emitCheckpoint(long windowId)
  {
    CheckpointTuple ct = new CheckpointTuple(windowId);
    ct.setWindowId(currentWindowId);
    for (final Sink<Object> output: outputs.values()) {
      output.put(ct);
    }
  }

  protected void handleRequests(long windowId)
  {
    endWindowEmitTime = System.currentTimeMillis();
    OperatorStats stats = new OperatorStats();
    reportStats(stats);
    stats.checkpointedWindowId = checkpointedWindowId;
    context.report(stats, windowId);

    /*
     * we prefer to cater to requests at the end of the window boundary.
     */
    try {
      BlockingQueue<OperatorContext.NodeRequest> requests = context.getRequests();
      int size;
      if ((size = requests.size()) > 0) {
        while (size-- > 0) {
          //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
          requests.remove().execute(operator, context.getId(), windowId);
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void reportStats(OperatorStats stats)
  {
    stats.outputPorts = new ArrayList<OperatorStats.PortStats>();
    for (Entry<String, Sink<Object>> e: outputs.entrySet()) {
      //logger.info("end window emit time is {}", endWindowEmitTime);
      stats.outputPorts.add(new OperatorStats.PortStats(e.getKey(), e.getValue().getCount(true), endWindowEmitTime));
    }

    long currentCpuTime = tmb.getCurrentThreadCpuTime();
    stats.cpuTimeUsed = currentCpuTime - lastSampleCpuTime;
    lastSampleCpuTime = currentCpuTime;
  }

  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = Sink.NO_SINKS;
    }
    else {
      @SuppressWarnings("unchecked")
      Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, size);
      for (Sink<Object> s: outputs.values()) {
        newSinks[--size] = s;
      }

      sinks = newSinks;
    }
  }

  protected void deactivateSinks()
  {
    sinks = Sink.NO_SINKS;
  }

  public boolean isAlive()
  {
    return alive;
  }

  public long getBackupWindowId()
  {
    return checkpointedWindowId;
  }

  private static final Logger logger = LoggerFactory.getLogger(Node.class);
}
