/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.InputOperator;
import com.malhartech.tuple.Tuple;
import com.malhartech.util.AttributeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputNode extends Node<InputOperator>
{
  protected Reservoir controlTuples;

  public InputNode(String id, InputOperator operator)
  {
    super(id, operator);
  }

  @Override
  public void connectInputPort(String port, AttributeMap<PortContext> attributes, Reservoir reservoir)
  {
    if (Node.INPUT.equals(port)) {
      controlTuples = reservoir;
    }
  }

  @Override
  @SuppressWarnings(value = "SleepWhileInLoop")
  public final void run()
  {
    long spinMillis = context.getAttributes().attrValue(OperatorContext.SPIN_MILLIS, 10);
    boolean insideWindow = false;
    int windowCount = 0;

    Tuple t;
    try {
      while (alive) {
        int size;
        if ((size = controlTuples.size()) > 0) {
          while (size-- > 0) {
            t = (Tuple)controlTuples.remove();
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
                handleRequests(currentWindowId, false);
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

            for (InternalCounterSink cs: sinks) {
              generatedTuples -= cs.getCount();
            }

            operator.emitTuples();

            for (InternalCounterSink cs: sinks) {
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
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
}
