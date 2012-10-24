/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operator.Port;
import com.malhartech.api.Sink;
import com.malhartech.api.Stats;
import com.malhartech.api.Stats.StatsReporter;
import com.malhartech.dag.Operators.PortMappingDescriptor;
import com.malhartech.util.CircularBuffer;
import java.util.*;
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
  protected final HashMap<String, StatsReporterSink<?>> outputs = new HashMap<String, StatsReporterSink<?>>();
  protected final int spinMillis = 10;
  protected final int bufferCapacity = 1024 * 1024;
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile StatsReporterSink[] sinks = StatsReporterSink.NO_SINKS;
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

  protected void connectOutputPort(String port, final Sink sink)
  {
    OutputPort outputPort = descriptor.outputPorts.get(port);
    if (outputPort != null) {
      outputPort.setSink(sink);

      if (sink instanceof StatsReporter) {
        outputs.put(port, (StatsReporterSink<?>)sink);
      }
      else if (sink == null) {
        outputs.remove(port);
      }
      else {
        outputs.put(port, new StatsReporterSink()
        {
          int count;

          @Override
          public void process(Object tuple)
          {
            count++;
            sink.process(tuple);
          }

          @Override
          public Stats getStats(String id)
          {
            PortStats ps = new PortStats(id, count);
            count = 0;
            return ps;
          }
        });
      }
    }
  }

  public abstract Sink connect(String id, Sink sink);

  protected void activateSinks()
  {
    StatsReporterSink[] newSinks = new StatsReporterSink[outputs.size()];

    int i = 0;
    for (StatsReporterSink s: outputs.values()) {
      newSinks[i++] = s;
    }

    this.sinks = newSinks;
  }

  public void deactivateSinks()
  {
    sinks = StatsReporterSink.NO_SINKS;
    outputs.clear();
  }
  OperatorContext context;

  public void activate(OperatorContext context)
  {
    boolean activationListener = operator instanceof ActivationListener;

    activateSinks();
    alive = true;
    this.context = context;

    if (activationListener) {
      ((ActivationListener)operator).postActivate(context);
    }

    run();

    if (activationListener) {
      ((ActivationListener)operator).preDeactivate();
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
    logger.debug("{} sending EndOfStream", this);
    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    // we need to think about this as well.
    EndStreamTuple est = new EndStreamTuple();
    for (final Sink output: outputs.values()) {
      output.process(est);
    }
  }

  protected void handleRequests(long windowId)
  {
    /*
     * we prefer to cater to requests at the end of the window boundary.
     */
    try {
      CircularBuffer<OperatorContext.ModuleRequest> requests = context.getRequests();
      for (int i = requests.size(); i-- > 0;) {
        //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
        requests.remove().execute(operator, context.getId(), windowId);
      }
    }
    catch (Exception e) {
      logger.warn("Exception while catering to external request {}", e);
    }

    HashMap<String, Collection<Stats>> stats = new HashMap<String, Collection<Stats>>();
    reportStats(stats);

    Stats operatorStats = new Stats()
    {
    };
    context.report(0, 0L, windowId);
//    generatedTupleCount = 0;
  }

  protected void reportStats(Map<String, Collection<Stats>> stats)
  {
    ArrayList<Stats> opstats = new ArrayList<Stats>();
    for (Entry<String, StatsReporterSink<?>> e: outputs.entrySet()) {
      opstats.add(e.getValue().getStats(e.getKey()));
    }

    stats.put("Output Ports", opstats);
  }
}
