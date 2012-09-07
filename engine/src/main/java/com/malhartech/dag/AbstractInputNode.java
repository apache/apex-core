/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
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
  private transient String id;
  private transient HashMap<String, CircularBuffer<Object>> afterBeginWindows;
  private transient HashMap<String, CircularBuffer<Tuple>> afterEndWindows;
  private transient HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  private transient volatile Sink[] sinks = new Sink[0];
  private transient NodeContext ctx;
  private transient int producedTupleCount;
  private transient int spinMillis;
  private transient int bufferCapacity;

  @Override
  public void setup(NodeConfiguration config)
  {
    id = config.get("id");
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
    sinks = new Sink[outputs.size()];

    int i = 0;
    for (Sink s: outputs.values()) {
      sinks[i++] = s;
    }
  }

  @Override
  public void deactivate()
  {
    sinks = new Sink[0];
  }

  @Override
  public void teardown()
  {
    outputs.clear();

    afterEndWindows.clear();
    afterEndWindows = null;
    afterBeginWindows.clear();
    afterBeginWindows = null;
    // Should make variable "shutdown" part of AbstrastInputNode, users should not have to override teardown()
    // as they may forget to call super.teardown()
    // Also move "outputconnected" here as that is a very common need
  }

  @Override
  public final Sink connect(String port, Sink component)
  {
    Sink retvalue;
    if (Component.INPUT.equals(port)) {
      retvalue = this;
    }
    else if (component == null) {
      outputs.remove(port);
      retvalue = null;
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
    int i;
    Tuple t = (Tuple)payload;
    switch (t.getType()) {
      case BEGIN_WINDOW:
        i = sinks.length;
        do {
          try {
            while (i-- > 0) {
              sinks[i].process(payload);
            }
          }
          catch (MutatedSinkException mse) {
            final Sink newSink = mse.getNewSink();
            newSink.process(payload);
            sinks[i] = newSink;
            replaceOutput(mse.getOldSink(), newSink);
          }
        }
        while (i > 0);

        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          final Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        break;

      case END_WINDOW:
        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          final Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        for (final Sink s: sinks) {
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
          final Sink s = outputs.get(e.getKey());
          CircularBuffer<?> cb = e.getValue();
          for (i = cb.size(); i > 0; i--) {
            s.process(cb.get());
          }
        }
        break;

      default:
        for (final Sink s: sinks) {
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

  @Override
  public int hashCode()
  {
    return id == null ? super.hashCode() : id.hashCode();
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
    final AbstractInputNode other = (AbstractInputNode)obj;
    if ((this.id == null) ? (other.id != null) : !this.id.equals(other.id)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" + "id=" + id + ", outputs=" + outputs.keySet() + '}';
  }

  private void replaceOutput(Sink oldSink, Sink newSink)
  {
    for (Entry<String, Sink> e: outputs.entrySet()) {
      if (e.getValue() == oldSink) {
        outputs.put(e.getKey(), newSink);
        break;
      }
    }
  }
}
