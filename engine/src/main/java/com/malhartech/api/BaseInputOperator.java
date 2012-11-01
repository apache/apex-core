/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for input operator with a single output port. Handles hand over
 * from asynchronous input to port processing thread (tuples must be emitted by
 * container thread). If derived class implements {
 *
 * @Runnable} to
 * perform synchronous IO, this class will manage the thread according
 * to the operator lifecycle.
 */
public class BaseInputOperator<T> extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(BaseInputOperator.class);
  private transient Thread ioThread;
  private transient boolean isActive = false;
  private long currentWindowId;
  /**
   * The single output port of this input operator.
   * Collects asynchronously emitted tuples and flushes in container thread.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient BufferingOutputPort<T> outputPort = new BufferingOutputPort<T>(this);

  @Override
  final public void postActivate(OperatorContext ctx)
  {
    isActive = true;
    if (this instanceof Runnable) {
      ioThread = new Thread((Runnable)this, "io-" + this.getName());
      ioThread.start();
    }
  }

  @Override
  final public void preDeactivate()
  {
    isActive = false;
    if (ioThread != null) {
      // thread to exit any wait state due to sleep or blocking IO
      ioThread.interrupt();
    }
  }

  final public boolean isActive()
  {
    return isActive;
  }

  @Override
  public void emitTuples()
  {
    outputPort.flush();
  }

  public static class CollectorSink<T> implements Sink<T>
  {
    public ArrayList<T> tuples = new ArrayList<T>();

    @Override
    public synchronized void process(T tuple)
    {
      tuples.add(tuple);
    }

    public synchronized void drainTo(Sink<T> sink)
    {
      for (T tuple: tuples) {
        sink.process(tuple);
      }
      tuples.clear();
    }
  }

  public static class BufferingOutputPort<T> extends DefaultOutputPort<T>
  {
    private transient final CollectorSink<T> bufferingSink = new CollectorSink<T>();

    /**
     * @param operator
     */
    public BufferingOutputPort(Operator operator)
    {
      super(operator);
    }

    @Override
    public void emit(T tuple)
    {
      bufferingSink.process(tuple);
    }

    public void flush()
    {
      for (T tuple: bufferingSink.tuples) {
        super.emit(tuple);
      }
    }
  };
}