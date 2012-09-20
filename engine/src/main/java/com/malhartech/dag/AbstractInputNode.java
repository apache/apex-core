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

// write recoverable AIN
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputNode extends AbstractBaseModule implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputNode.class);
  private transient HashMap<String, CircularBuffer<Object>> afterBeginWindows;
  private transient HashMap<String, CircularBuffer<Tuple>> afterEndWindows;
  private transient NodeContext ctx;

  public AbstractInputNode()
  {
    afterBeginWindows = new HashMap<String, CircularBuffer<Object>>();
    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public final void activate(NodeContext context)
  {
    ctx = context;
    activateSinks();
    run();

    try {
      EndStreamTuple est = new EndStreamTuple();
      for (CircularBuffer<Tuple> cb: afterEndWindows.values()) {
        while (true) {
          try {
            cb.add(est);
            break;
          }
          catch (BufferOverflowException boe) {
            Thread.sleep(spinMillis);
          }
        }
      }

      /*
       * make sure that it's sent.
       */
      boolean pendingMessages;
      do {
        Thread.sleep(spinMillis);

        pendingMessages = false;
        for (CircularBuffer<Tuple> cb: afterEndWindows.values()) {
          if (cb.size() > 0) {
            pendingMessages = true;
            break;
          }
        }
      }
      while (pendingMessages && sinks.length > 0);
    }
    catch (InterruptedException ex) {
      logger.info("Not waiting for the emitted tuples to be flushed as got interrupted by {}", ex.getLocalizedMessage());
    }
  }

  @Override
  public final void deactivate()
  {
    super.deactivate();
  }

  @Override
  public final Sink connect(String port, Sink component)
  {
    Sink retvalue;
    if (Component.INPUT.equals(port)) {
      retvalue = this;
    }
    else {
      PortAnnotation pa = getPort(port);
      if (pa == null) {
        throw new IllegalArgumentException("Unrecognized Port " + port + " for " + this);
      }

      port = pa.name();
      if (component == null) {
        outputs.remove(port);
        afterBeginWindows.remove(port);
      }
      else {
        outputs.put(port, component);
        afterBeginWindows.put(port, new CircularBuffer<Object>(bufferCapacity));
        afterEndWindows.put(port, new CircularBuffer<Tuple>(bufferCapacity));
      }

      if (sinks != NO_SINKS) {
        activateSinks();
      }
      retvalue = null;
    }

    connected(port, component);
    return retvalue;
  }

  @Override
  @SuppressWarnings("SillyAssignment")
  public final void process(Object payload)
  {
    Tuple t = (Tuple)payload;
    switch (t.getType()) {
      case BEGIN_WINDOW:
        beginWindow();
        for (int i = sinks.length; i-- > 0;) {
          sinks[i].process(payload);
        }

        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          final Sink s = outputs.get(e.getKey());
          if (s != null) {
            CircularBuffer<?> cb = e.getValue();
            for (int i = cb.size(); i-- > 0;) {
              s.process(cb.get());
            }
          }
        }
        break;

      case END_WINDOW:
        for (Entry<String, CircularBuffer<Object>> e: afterBeginWindows.entrySet()) {
          final Sink s = outputs.get(e.getKey());
          if (s != null) {
            CircularBuffer<?> cb = e.getValue();
            for (int i = cb.size(); i-- > 0;) {
              s.process(cb.get());
            }
          }
        }
        endWindow();
        for (int i = sinks.length; i-- > 0;) {
          sinks[i].process(payload);
        }

        ctx.report(processedTupleCount, 0L, ((Tuple)payload).getWindowId());
        processedTupleCount = 0;

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
          if (s != null) {
            CircularBuffer<?> cb = e.getValue();
            for (int i = cb.size(); i-- > 0;) {
              s.process(cb.get());
            }
          }
        }
        break;

      default:
        for (int i = sinks.length; i-- > 0;) {
          sinks[i].process(payload);
        }
    }
  }

  @SuppressWarnings("SleepWhileInLoop")
  public void emit(String id, Object payload)
  {
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

    processedTupleCount++;
  }
}
