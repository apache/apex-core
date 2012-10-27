/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.deprecated.dag;

import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Sink;
import com.malhartech.dag.CounterSink;
import com.malhartech.dag.InputNode;
import com.malhartech.dag.OperatorContextImpl;
import com.malhartech.deprecated.api.SyncInputOperator;
import com.malhartech.deprecated.dag.SyncInputNode.SyncSink;
import com.malhartech.util.CircularBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module bridges the gap between the synchronous data sources and InputNode which
 * requires that the tuples be emitted in the process method as quickly as possible and return.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class SyncInputNode extends InputNode<SyncInputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(SyncInputNode.class);
  protected Thread syncThread;
  @SuppressWarnings("VolatileArrayField")
  volatile SyncSink[] syncsinks;

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
      retvalue = super.connect(id, sink == null ? null : new SyncSink(sink, bufferCapacity));
    }

    return retvalue;
  }

  @Override
  public void activate(OperatorContextImpl context)
  {
    syncThread = new Thread(operator.getDataPoller(), operator.toString());
    syncThread.start();
    super.activate(context);
  }

  @Override
  protected void activateSinks()
  {
    super.activateSinks();
    /*
     * Casting is costly, so do it on rare occassions and save the performant regions.
     */
    if (sinks == CounterSink.NO_SINKS) {
      syncsinks = new SyncSink[0];
    }
    else {
      SyncSink[] newSinks = new SyncSink[sinks.length];
      for (int i = sinks.length; i-- > 0;) {
        newSinks[i] = (SyncSink)sinks[i];
      }
      syncsinks = newSinks;
    }
  }

  @Override
  public void deactivate()
  {
    logger.debug("interrupting {}", syncThread);
    syncThread.interrupt();
    syncThread = null;
    super.deactivate();
  }

  @Override
  protected final void emitTuples() throws InterruptedException
  {
    for (int i = syncsinks.length; i-- > 0;) {
      syncsinks[i].sweep();
    }
  }

  class SyncSink extends CircularBuffer<Object> implements CounterSink<Object>
  {
    final Sink sink;
    int count;

    public SyncSink(Sink sink, int buffersize)
    {
      super(buffersize);
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
      int size = size();
      count += size;
      while (size-- > 0) {
        sink.process(pollUnsafe());
      }
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

  }
}
