/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.InputOperator;
import com.malhartech.api.Operator;
import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import java.util.Map.Entry;
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
  public Sink connect(String port, final Sink sink)
  {
    Sink retvalue;
    if (Node.INPUT.equals(port)) {
      retvalue = new Sink()
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
    else {
      connectOutputPort(port, sink);
      retvalue = null;
    }

    return retvalue;
  }

  @Override
  @SuppressWarnings(value = "SleepWhileInLoop")
  public final void run()
  {
    boolean inWindow = false;
    Tuple t = null;
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
                inWindow = true;
                currentWindowId = t.getWindowId();
                operator.beginWindow(currentWindowId);
                break;

              case END_WINDOW:
                operator.endWindow();
                inWindow = false;
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
          if (inWindow) {
            int generatedTuples = 0;

            for (CounterSink cs: sinks) {
              generatedTuples -= cs.getCount();
            }

            operator.emitTuples();

            for (CounterSink cs: sinks) {
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

    if (inWindow) {
      EndWindowTuple ewt = new EndWindowTuple();
      ewt.setWindowId(t.getWindowId());
      for (final Sink output: outputs.values()) {
        output.process(ewt);
      }
    }
  }
}
