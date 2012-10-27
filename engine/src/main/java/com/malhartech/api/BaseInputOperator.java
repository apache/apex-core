/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.api;

import java.util.ArrayList;

import com.malhartech.annotation.OutputPortFieldAnnotation;

/**
 * Base class for input operator with a single output port. Handles hand over
 * from asynchronous input to port processing thread (tuples must be emitted by
 * container thread). If derived class implements {
 *
 * @Runnable} to
 * perform synchronous IO, this class will manage the thread according
 * to the operator lifecycle.
 */
public class BaseInputOperator<T> extends BaseOperator implements AsyncInputOperator, ActivationListener<Context>
{
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
  final public void postActivate(Context ctx)
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
  final public void emitTuples(long windowId)
  {
    if (windowId < currentWindowId) {
      emitPreviousWindowTuples(windowId);
      this.outputPort.flush();
    }
    else {
      this.outputPort.flush();
      currentWindowId = windowId;
    }
  }

  /**
   * Callback on replay of previous window (during recovery).
   * If subclass has the ability to recover the input, emit to port.
   *
   * @param windowId
   * @param sink
   */
  public void emitPreviousWindowTuples(long windowId)
  {
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