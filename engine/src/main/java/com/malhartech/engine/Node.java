/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operator.Port;
import com.malhartech.api.Sink;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.stram.TupleRecorder.RecorderSink;
import com.malhartech.util.AttributeMap;
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
  private static final Logger logger = LoggerFactory.getLogger(Node.class);
  /*
   * if the Component is capable of taking only 1 input, call it INPUT.
   * if the Component is capable of providing only 1 output, call it OUTPUT.
   */
  public static final String INPUT = "input";
  public static final String OUTPUT = "output";
  public final String id;
  protected final HashMap<String, InternalCounterSink> outputs = new HashMap<String, InternalCounterSink>();
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile InternalCounterSink[] sinks = InternalCounterSink.NO_SINKS;
  protected final int spinMillis = 10;
  protected boolean alive;
  protected final OPERATOR operator;
  protected final PortMappingDescriptor descriptor;
  protected long currentWindowId;
  protected int applicationWindowCount;
  protected long stramWindowSize;
  protected long beginWindowTime = 0;
  protected long endWindowTime = 0;

  public Node(String id, OPERATOR operator)
  {
    this.id = id;
    this.operator = operator;

    descriptor = new PortMappingDescriptor();
    Operators.describe(operator, descriptor);
  }

  public Operator getOperator()
  {
    return operator;
  }

  public PortMappingDescriptor getPortMappingDescriptor()
  {
    return descriptor;
  }

  public void connectOutputPort(String port, AttributeMap<PortContext> attributes, final Sink<Object> sink)
  {
    @SuppressWarnings("unchecked")
    OutputPort<Object> outputPort = (OutputPort<Object>)descriptor.outputPorts.get(port);
    if (outputPort != null) {
      if (sink instanceof InternalCounterSink) {
        outputPort.setSink(sink);
        outputs.put(port, (InternalCounterSink)sink);
      }
      else if (sink == null) {
        outputPort.setSink(null);
        outputs.remove(port);
      }
      else {
        InternalCounterSink cs = new InternalCounterSink(sink);
        outputPort.setSink(cs);
        outputs.put(port, cs);
      }
    }
  }

  public abstract Sink<Object> connectInputPort(String port, AttributeMap<PortContext> attributes, final Sink<? extends Object> sink);

  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
      InternalCounterSink ics = outputs.get(e.getKey());
      if (ics != null) {
        ics.sink = new ForkingSink(ics.sink, e.getValue());
      }
    }

  }

  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    for (Entry<String, Sink<Object>> e: sinks.entrySet()) {
      InternalCounterSink ics = outputs.get(e.getKey());
      if (ics != null && ics.sink instanceof ForkingSink) {
        assert (((ForkingSink)ics.sink).second == e.getValue());
        ics.sink = ((ForkingSink)ics.sink).first;
      }
    }
  }

  OperatorContext context;

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
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Node<?> other = (Node<?>)obj;
    if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return id == null ? super.hashCode() : id.hashCode();
  }

  @Override
  public String toString()
  {
    return id;
  }

  protected void emitEndStream()
  {
//    logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    EndStreamTuple est = new EndStreamTuple();
    est.windowId = currentWindowId;
    for (final InternalCounterSink output: outputs.values()) {
      output.process(est);
    }
  }

  protected void emitEndWindow()
  {
    EndWindowTuple ewt = new EndWindowTuple();
    ewt.setWindowId(currentWindowId);
    for (final Sink<Object> output: outputs.values()) {
      output.process(ewt);
    }
  }

  public void emitCheckpoint(long windowId)
  {
    CheckpointTuple ct = new CheckpointTuple();
    ct.windowId = currentWindowId;
    for (final InternalCounterSink output: outputs.values()) {
      output.process(ct);
    }
  }

  protected void handleRequests(long windowId, boolean applicationWindowBoundary)
  {
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

    OperatorStats stats = new OperatorStats();
    reportStats(stats, applicationWindowBoundary);

    context.report(stats, windowId);
  }

  protected void reportStats(OperatorStats stats, boolean applicationWindowBoundary)
  {
    stats.outputPorts = new ArrayList<OperatorStats.PortStats>();
    for (Entry<String, InternalCounterSink> e: outputs.entrySet()) {
      stats.outputPorts.add(new OperatorStats.PortStats(e.getKey(), e.getValue().resetCount()));
    }
    if (applicationWindowBoundary) {
      stats.latency = new Long(endWindowTime - beginWindowTime - applicationWindowCount * stramWindowSize * 1000000);
    }
  }

  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = InternalCounterSink.NO_SINKS;
    }
    else {
      @SuppressWarnings("unchecked")
      InternalCounterSink[] newSinks = (InternalCounterSink[])Array.newInstance(InternalCounterSink.class, size);
      for (InternalCounterSink s: outputs.values()) {
        newSinks[--size] = s;
      }

      sinks = newSinks;
    }
  }

  protected void deactivateSinks()
  {
    sinks = InternalCounterSink.NO_SINKS;
  }

  public boolean isAlive()
  {
    return alive;
  }

  static class ForkingSink implements Sink<Object>
  {
    final Sink<Object> first;
    final Sink<Object> second;

    ForkingSink(Sink<Object> f, Sink<Object> s)
    {
      first = f;
      second = s;
    }

    @Override
    public void process(Object tuple)
    {
      first.process(tuple);
      second.process(tuple);
    }

  }

  protected static class InternalCounterSink implements Sink<Object>
  {
    @SuppressWarnings({"FieldNameHidesFieldInSuperclass", "VolatileArrayField"})
    public static final InternalCounterSink[] NO_SINKS = new InternalCounterSink[0];
    int count;
    Sink<Object> sink;

    InternalCounterSink(Sink<Object> sink)
    {
      this.sink = sink;
    }

    @Override
    public void process(Object tuple)
    {
      count++;
      sink.process(tuple);
    }

    public int getCount()
    {
      return count;
    }

    public int resetCount()
    {
      int ret = count;
      count = 0;
      return ret;
    }

  }

}
