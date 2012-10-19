/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Sink;
import com.malhartech.api.SyncInputOperator;
import com.malhartech.dag.SyncInputNode.SyncSink;
import com.malhartech.util.CircularBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module bridges the gap between the synchronous data sources and InputNode which
 * requires that the tuples be emitted in the process method as quickly as possible and return.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class SyncInputNode extends InputNode<SyncInputOperator, SyncSink>
{
  private static final Logger logger = LoggerFactory.getLogger(SyncInputNode.class);
  protected Thread syncThread;

  public SyncInputNode(String id, SyncInputOperator operator)
  {
    super(id, operator);
  }

  @Override
  public Sink connect(String id, final Sink sink)
  {
    Sink retvalue;

    OutputPort port = descriptor.outputPorts.get(id);
    if (port == null) {
      retvalue = super.connect(id, sink);
    }
    else {
      retvalue = super.connect(id, sink == null ? null : new SyncSink(id, sink, bufferCapacity));
    }

    return retvalue;
  }

  @Override
  public void activate(OperatorContext context)
  {
    syncThread = new Thread(operator.getDataPoller(), operator.toString());
    syncThread.start();
    super.activate(context);
  }

  @Override
  public void deactivate()
  {
    syncThread.interrupt();
    syncThread = null;
    super.deactivate();
  }

  @Override
  protected void injectTuples() throws InterruptedException
  {
    int oldg = generatedTupleCount;

    for (SyncSink s: outputs.values()) {
      s.sweep();
    }

    if (generatedTupleCount == oldg) {
      Thread.sleep(spinMillis);
    }
  }

  class SyncSink extends CircularBuffer<Object> implements Sink
  {
    final String id;
    final Sink sink;

    public SyncSink(String id, Sink sink, int buffersize)
    {
      super(buffersize);
      this.id = id;
      this.sink = sink;
    }

    @Override
    public final void process(Object tuple)
    {
      try {
        put(tuple);
      }
      catch (InterruptedException ex) {
        logger.warn("{} aborting emit as got interrupted while writing {}", this, tuple);
      }
    }

    public final void sweep()
    {
      for (int i = size(); i-- > 0;) {
        sink.process(pollUnsafe());
      }
    }
  }
}
