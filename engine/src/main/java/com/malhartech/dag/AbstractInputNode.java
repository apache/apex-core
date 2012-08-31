/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputNode implements Node
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputNode.class);
  int spinMillis;
  int bufferCapacity;
  HashMap<String, CircularBuffer<Object>> afterBeginWindows;
  HashMap<String, CircularBuffer<Tuple>> afterEndWindows;
  HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  Collection<Sink> sinks;
  private NodeContext ctx;
  private int producedTupleCount;

  @Override
  public void setup(NodeConfiguration config)
  {
    spinMillis = config.getInt("SpinMillis", 100);
    bufferCapacity = config.getInt("BufferCapacity", 1024);

    afterBeginWindows = new HashMap<String, CircularBuffer<Object>>();
    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();

    Class<? extends Node> clazz = this.getClass();
    NodeAnnotation na = clazz.getAnnotation(NodeAnnotation.class);
    if (na != null) {
      PortAnnotation[] ports = na.ports();
      for (PortAnnotation pa: ports) {
        if (pa.type() == PortAnnotation.PortType.OUTPUT || pa.type() == PortAnnotation.PortType.BIDI) {
          afterBeginWindows.put(pa.name(), new CircularBuffer<Object>(bufferCapacity));
          afterEndWindows.put(pa.name(), new CircularBuffer<Tuple>(bufferCapacity));
        }
      }
    }
  }

  @Override
  public void activate(NodeContext context)
  {
    ctx = context;
    sinks = outputs.values();
  }

  @Override
  public void deactivate()
  {
    sinks = Collections.emptyList();
  }

  @Override
  public void teardown()
  {
    outputs.clear();

    afterEndWindows.clear();
    afterEndWindows = null;
    afterBeginWindows.clear();
    afterBeginWindows = null;
  }

  @Override
  public final Sink connect(String port, Sink component)
  {
    Sink retvalue;
    if ("input".equals(port)) {
      retvalue = this;
    }
    else {
      outputs.put(port, component);
      retvalue = null;
    }

    connected(port, component);
    return retvalue;
  }

  public void connected(String id, Sink dagpart)
  {
    /* implementation to be optionally overridden by the user */
  }

  @Override
  public final void process(Object payload)
  {
    Tuple t = (Tuple)payload;
    switch (t.getType()) {
      case BEGIN_WINDOW:
        for (Sink s: sinks) {
          s.process(payload);
        }
        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (int i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        break;

      case END_WINDOW:
        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (int i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        for (Sink s: sinks) {
          s.process(payload);
        }

        ctx.report(producedTupleCount, 0L, ((Tuple)payload).getWindowId());
        producedTupleCount = 0;

        // the default is UNSPECIFIED which we ignore anyways as we ignore everything
        // that we do not understand!
        try {
          switch (ctx.getRequestType()) {
            case BACKUP:
              ctx.backup(this, ((Tuple)payload).getWindowId());
              break;

            case RESTORE:
              logger.info("restore requests are not implemented");
              break;
          }
        }
        catch (Exception e) {
          logger.warn("Exception while catering to external request", e.getLocalizedMessage());
        }

        // i think there should be just one queue instead of one per port - lets defer till we find an example.
        for (Entry<String, CircularBuffer<Tuple>> e: afterEndWindows.entrySet()) {
          Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (int i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        break;

      default:
        for (Sink s: sinks) {
          s.process(payload);
        }
    }
  }

  @SuppressWarnings("SleepWhileInLoop")
  public void emit(String id, Object payload)
  {
    // once we have annotation done property, we would like to send the tuples to
    // a queue corresponding to the port instead of in one. the followig implementation
    // ignores the id, that means it's good only for the nodes which have one o/p port.
    if (payload instanceof Tuple) {
      while (true) {
        try {
          afterEndWindows.get(id).add((Tuple)payload);
          break;
        }
        catch (BufferOverflowException ex) {
          try {
            Thread.sleep(spinMillis);
          }
          catch (InterruptedException ex1) {
            break;
          }
        }
      }
    }
    else {
      while (true) {
        try {
          afterBeginWindows.get(id).add(payload);
          break;
        }
        catch (BufferOverflowException ex) {
          try {
            Thread.sleep(spinMillis);
          }
          catch (InterruptedException ex1) {
            break;
          }
        }
      }
    }

    producedTupleCount++;
  }
}
