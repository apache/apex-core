/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputNode implements Node
{
  int spinMillis;
  int bufferCapacity;
  CircularBuffer afterBeginWindow;
  CircularBuffer<Tuple> afterEndWindow;
  HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  Collection<Sink> sinks;

  @Override
  public void setup(NodeConfiguration config)
  {
    spinMillis = config.getInt("SpinMillis", 100);
    bufferCapacity = config.getInt("BufferCapacity", 1024);
    afterBeginWindow = new CircularBuffer(bufferCapacity);
    afterEndWindow = new CircularBuffer<Tuple>(bufferCapacity);
  }

  @Override
  public void activate(NodeContext context)
  {
    sinks = outputs.values();
  }

  @Override
  public void deactivate()
  {
    sinks = Collections.EMPTY_LIST;
  }

  @Override
  public void teardown()
  {
    outputs.clear();
    afterEndWindow = null;
    afterBeginWindow = null;
  }

  @Override
  public Sink connect(String id, DAGComponent component)
  {
    if ("input".equals(id)) {
      return this;
    }
    else {
      outputs.put(id, component);
      return null;
    }
  }

  @Override
  public void process(Object payload)
  {
    Tuple t = (Tuple)payload;
    switch (t.getType()) {
      case BEGIN_WINDOW:
        for (Sink s: sinks) {
          s.process(payload);
        }
        for (int i = afterBeginWindow.size(); i > 0; i--) {
          Object o = afterBeginWindow.get();
          for (Sink s: sinks) {
            s.process(o);
          }
        }
        break;

      case END_WINDOW:
        for (int i = afterBeginWindow.size(); i > 0; i--) {
          Object o = afterBeginWindow.get();
          for (Sink s: sinks) {
            s.process(o);
          }
        }
        for (Sink s: sinks) {
          s.process(payload);
        }
        for (int i = afterEndWindow.size(); i > 0; i--) {
          Tuple o = afterEndWindow.get();
          o.setWindowId(t.getWindowId());
          for (Sink s: sinks) {
            s.process(t);
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
          afterEndWindow.add((Tuple)payload);
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
          afterBeginWindow.add(payload);
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
  }
}
