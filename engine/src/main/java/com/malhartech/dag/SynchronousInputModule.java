/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.PortAnnotation;
import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module bridges the gap between the synchronous data sources and InputModule which
 * requires that the tuples be emitted in the process method as quickly as possible and return.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class SynchronousInputModule extends InputModule implements Runnable, Sink
{
  private static final Logger logger = LoggerFactory.getLogger(SynchronousInputModule.class);
  protected transient HashMap<String, CircularBuffer<Object>> handoverBuffers = new HashMap<String, CircularBuffer<Object>>();
  protected transient Thread syncThread;

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void connected(String id, Sink dagpart)
  {
    PortAnnotation port = getPort(id);
    if (port != null && (port.type() == PortAnnotation.PortType.OUTPUT || port.type() == PortAnnotation.PortType.BIDI)) {
      CircularBuffer<?> cb = handoverBuffers.get(port.name());
      if (dagpart == null) {
        /* this is remove request */
        if (cb != null) {
          try {
            while (cb.size() > 0) {
              Thread.sleep(spinMillis);
            }
          }
          catch (InterruptedException ie) {
            logger.info("{} aborting handing over messages downstream due to interrupt", this);
          }
        }
      }
      else if (cb == null) {
        /* this is a new connection request */
        handoverBuffers.put(port.name(), new CircularBuffer<Object>(bufferCapacity));
      }
    }
  }

  @Override
  public void activated(OperatorContext context)
  {
    syncThread = new Thread(this, this + "-sync");
    syncThread.start();
  }

  @Override
  public void deactivated(OperatorContext context)
  {
    syncThread.interrupt();
    syncThread = null;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void emit(Object payload)
  {
    try {
      for (CircularBuffer<Object> cb: handoverBuffers.values()) {
        cb.put(payload);
      }
    }
    catch (InterruptedException ex) {
      logger.warn("{} aborting emit as got interrupted while writing {}", this, payload);
    }
  }

  @Override
  public void emit(String id, Object payload)
  {
    CircularBuffer<Object> cb = handoverBuffers.get(id);
    if (cb != null) {
      try {
        cb.put(payload);
      }
      catch (InterruptedException ex) {
        logger.warn("{} aborting emit as got interrupted while writing {}", this, payload);
      }
    }
  }

  @Override
  final public void process(Object payload)
  {
    for (Entry<String, CircularBuffer<Object>> e: handoverBuffers.entrySet()) {
      Sink s = outputs.get(e.getKey());
      CircularBuffer<?> cb = e.getValue();
      for (int i = cb.size(); i-- > 0;) {
        s.process(cb.poll());
      }
    }
  }
}
