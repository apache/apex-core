/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Sink;
import com.malhartech.engine.Operators.PortMappingDescriptor;
import com.malhartech.util.CircularBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
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
  protected final HashMap<String, CounterSink<?>> outputs = new HashMap<String, CounterSink<?>>();
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile CounterSink[] sinks = CounterSink.NO_SINKS;
  protected final int spinMillis = 10;
  protected final int bufferCapacity = 1024 * 1024;
  protected boolean alive;
  protected final OPERATOR operator;
  protected final PortMappingDescriptor descriptor;
  protected long currentWindowId;

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

  public void connectOutputPort(String port, final Sink sink)
  {
    OutputPort<?> outputPort = descriptor.outputPorts.get(port);
    if (outputPort != null) {
      if (sink instanceof CounterSink) {
        outputPort.setSink(sink);
        outputs.put(port, (CounterSink<?>)sink);
      }
      else if (sink == null) {
        outputPort.setSink(null);
        outputs.remove(port);
      }
      else {
        /*
         * if streams implemented CounterSink, this would not get called.
         */
        CounterSink cs = new CounterSink()
        {
          int count;

          @Override
          public void process(Object tuple)
          {
            count++;
            sink.process(tuple);
          }

          @Override
          public int getCount()
          {
            return count;
          }

          @Override
          public int resetCount()
          {
            int ret = count;
            count = 0;
            return ret;
          }
        };
        outputPort.setSink(cs);
        outputs.put(port, cs);
      }
    }
  }

  public abstract Sink<?> connectInputPort(String port, final Sink sink);

  OperatorContext context;

  public void activate(OperatorContext context)
  {
    boolean activationListener = operator instanceof ActivationListener;

    activateSinks();
    alive = true;
    this.context = context;

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
    //logger.info("deactivated", new Exception());
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
    final Node other = (Node)obj;
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
    return id + ":" + operator.getClass().getSimpleName();
  }

  protected void emitEndStream()
  {
//    logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    EndStreamTuple est = new EndStreamTuple();
    for (final CounterSink output: outputs.values()) {
      output.process(est);
    }
  }

  protected void handleRequests(long windowId)
  {
    /*
     * we prefer to cater to requests at the end of the window boundary.
     */
    try {
      CircularBuffer<OperatorContext.NodeRequest> requests = context.getRequests();
      int size;
      if ((size = requests.size()) > 0 && !Thread.currentThread().isInterrupted()) {
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
    reportStats(stats);

    context.report(stats, windowId);
  }

  protected void reportStats(OperatorStats stats)
  {
    stats.ouputPorts = new ArrayList<OperatorStats.PortStats>();
    for (Entry<String, CounterSink<?>> e: outputs.entrySet()) {
      stats.ouputPorts.add(new OperatorStats.PortStats(e.getKey(), e.getValue().resetCount()));
    }
  }

  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = CounterSink.NO_SINKS;
    }
    else {
      CounterSink[] newSinks = new CounterSink[size];
      for (CounterSink s: outputs.values()) {
        newSinks[--size] = s;
      }

      sinks = newSinks;
    }
  }

  protected void deactivateSinks()
  {
    sinks = CounterSink.NO_SINKS;
  }

  public boolean isAlive()
  {
    return alive;
  }
}
