/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Operator.Port;
import com.malhartech.api.Sink;
import com.malhartech.dag.Operators.PortMappingDescriptor;
import com.malhartech.util.CircularBuffer;
import java.util.Collection;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class Node<OPERATOR extends Operator, SINK extends Sink> implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(Node.class);
  /*
   * if the Component is capable of taking only 1 input, call it INPUT.
   * if the Component is capable of providing only 1 output, call it OUTPUT.
   */
  public static final String INPUT = "input";
  public static final String OUTPUT = "output";
  public final String id;
  protected final HashMap<String, SINK> outputs = new HashMap<String, SINK>();
  protected int spinMillis = 10;
  protected int bufferCapacity = 1024 * 1024;
  protected int processedTupleCount;
  protected int generatedTupleCount;
  @SuppressWarnings(value = "VolatileArrayField")
  protected volatile SINK[] sinks = (SINK[])Sink.NO_SINKS;
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

  // The following 3 get*Port methods are just a placeholder.
  static <T extends Port> T getPort(Operator operator, String portname)
  {
    try {
      return (T)null;
    }
    catch (ClassCastException cce) {
      return null;
    }
  }

  static InputPort getInputPort(Operator operator, String portname)
  {
    return getPort(operator, portname);
  }

  static OutputPort getOutputPort(Operator operator, String portname)
  {
    return getPort(operator, portname);
  }

  static Collection<InputPort> getInputPorts(Operator operator)
  {
    return null;
  }

  public abstract Sink connect(String id, Sink sink);

  protected void activateSinks()
  {
    SINK[] newSinks = (SINK[])new Sink[outputs.size()];

    int i = 0;
    for (SINK s: outputs.values()) {
      newSinks[i++] = s;
    }

    this.sinks = newSinks;
  }

  public void deactivateSinks()
  {
    sinks = (SINK[])Sink.NO_SINKS;
    outputs.clear();
  }
  OperatorContext context;

  public void activate(OperatorContext context)
  {
    activateSinks();
    alive = true;
    operator.activated(context);

    this.context = context;
    run();
    this.context = null;

    operator.deactivated();
    emitEndStream();
    deactivateSinks();
  }

  public void deactivate()
  {
    alive = false;
  }

  /**
   * Emit the payload to all active output ports
   *
   * @param payload
   */
  public void emit(final Object payload)
  {
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(payload);
    }

    generatedTupleCount++;
  }

  /**
   * Emit the payload to the specified output port.
   *
   * It's expected that the output port is active, otherwise NullPointerException is thrown.
   *
   * @param id
   * @param payload
   */
  public void emit(String id, Object payload)
  {
    final Sink s = outputs.get(id);
    outputs.get(id).process(payload);
    generatedTupleCount++;
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

  /**
   * @return the bufferCapacity
   */
  public int getBufferCapacity()
  {
    return bufferCapacity;
  }

  /**
   * @return the spinMillis
   */
  public int getSpinMillis()
  {
    return spinMillis;
  }

  @Override
  public int hashCode()
  {
    return id == null ? super.hashCode() : id.hashCode();
  }

  /**
   * @param bufferCapacity the bufferCapacity to set
   */
  public void setBufferCapacity(int bufferCapacity)
  {
    this.bufferCapacity = bufferCapacity;
  }

  /**
   * @param spinMillis the spinMillis to set
   */
  public void setSpinMillis(int spinMillis)
  {
    this.spinMillis = spinMillis;
  }

  @Override
  public String toString()
  {
    return id + ":" + operator.getClass().getSimpleName();
  }

  /**
   * @return the processedTupleCount
   */
  public int getProcessedTupleCount()
  {
    return processedTupleCount;
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

    context.report(generatedTupleCount, 0L, windowId);
    generatedTupleCount = 0;
  }
}
