/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.PortAnnotation;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module bridges the gap between the synchronous data sources and AbstractInputModule which
 * requires that the tuples be emitted in the process method as quickly as possible and return.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractSynchronousInputModule extends AbstractInputModule implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSynchronousInputModule.class);
  protected transient Thread syncThread;
  protected transient HashMap<String, CircularBuffer> handoverBuffers = new HashMap<String, CircularBuffer>();

  @Override
  public void connected(String id, Sink dagpart)
  {
    PortAnnotation port = getPort(id);
    if (port != null && (port.type() == PortAnnotation.PortType.OUTPUT || port.type() == PortAnnotation.PortType.BIDI)) {
      if (dagpart == null) {
        handoverBuffers.remove(port.name()); // is it good thing to remove this?
      }
      else {
        handoverBuffers.put(port.name(), new CircularBuffer<Object>(bufferCapacity));
      }
    }
  }

  @Override
  public void activated(ModuleContext context)
  {
    syncThread = new Thread(this, this + "-sync");
    syncThread.start();
  }

  @Override
  public void deactivated(ModuleContext context)
  {
    syncThread.interrupt();
    syncThread = null;
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void emit(Object payload)
  {
    for (CircularBuffer cb: handoverBuffers.values()) {
      while (true) {
        try {
          cb.add(payload);
          break;
        }
        catch (BufferOverflowException boe) {
          try {
            Thread.sleep(spinMillis);
          }
          catch (InterruptedException ex) {
            logger.warn("{} aborting emit as got interrupted while writing {}", this, payload);
            break;
          }
        }
      }
    }
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void emit(String id, Object payload)
  {
    CircularBuffer cb = handoverBuffers.get(id);
    if (cb != null) {
      while (true) {
        try {
          cb.add(payload);
          break;
        }
        catch (BufferOverflowException boe) {
          try {
            Thread.sleep(spinMillis);
          }
          catch (InterruptedException ex) {
            logger.warn("{} aborting emit as got interrupted while writing {}", this, payload);
            break;
          }
        }
      }
    }
  }

  @Override
  public void process(Object payload)
  {
    for (Entry<String, CircularBuffer> e: handoverBuffers.entrySet()) {
      Sink s = outputs.get(e.getKey());
      CircularBuffer cb = e.getValue();
      for (int i = cb.size(); i-- > 0;) {
        s.process(cb.get());
      }
    }
  }
}
