/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.InputOperator;
import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputNode extends Node<InputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
  protected CircularBuffer<Tuple> controlTuples;

  public InputNode(String id, InputOperator operator)
  {
    super(id, operator);
    controlTuples = new CircularBuffer<Tuple>(1024);
  }

  @Override
  public Sink<Object> connectInputPort(String port, final Sink<? extends Object> sink)
  {
    if (Node.INPUT.equals(port)) {
      return new Sink<Object>()
      {
        @Override
        public void process(Object payload)
        {
          try {
            controlTuples.put((Tuple)payload);
          }
          catch (InterruptedException ex) {
            logger.debug("Got interrupted while putting {}", payload);
          }
        }

      };
    }

    return null;
  }

  @Override
  @SuppressWarnings(value = "SleepWhileInLoop")
  public final void run()
  {
    boolean insideWindow = false;
    int windowCount = 0;

    Tuple t;
    try {
      while (alive) {
        int size;
        if ((size = controlTuples.size()) > 0) {
          while (size-- > 0) {
            t = controlTuples.poll();
            switch (t.getType()) {
              case BEGIN_WINDOW:
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].process(t);
                }
                currentWindowId = t.getWindowId();
                if (windowCount == 0) {
                  insideWindow = true;
                  operator.beginWindow(currentWindowId);
                }
                operator.emitTuples(); /* give at least one change to emit the tuples */
                break;

              case END_WINDOW:
                if (++windowCount == applicationWindowCount) {
                  operator.endWindow();
                  insideWindow = false;
                  windowCount = 0;
                }
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].process(t);
                }
                handleRequests(currentWindowId);
                break;

              default:
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].process(t);
                }
                break;
            }
          }
        }
        else {
          if (insideWindow) {
            int generatedTuples = 0;

            for (CounterSink<Object> cs: sinks) {
              generatedTuples -= cs.getCount();
            }

            operator.emitTuples();

            for (CounterSink<Object> cs: sinks) {
              generatedTuples += cs.getCount();
            }

            if (generatedTuples == 0) {
              Thread.sleep(spinMillis);
            }
          }
          else {
            Thread.sleep(0);
          }
        }
      }
    }
    catch (InterruptedException ex) {
      alive = false;
    }
    catch (RuntimeException ex) {
      if (ex.getCause() instanceof InterruptedException) {
        alive = false;
      }
      else {
        throw ex;
      }
    }

    if (insideWindow) {
      operator.endWindow();
      //emitEndWindow();
    }
  }

  @Override
  public void addSinks(Map<String, Sink<Object>> sinks)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeSinks(Map<String, Sink<Object>> sinks)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
