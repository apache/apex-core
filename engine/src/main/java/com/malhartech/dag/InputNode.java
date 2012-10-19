/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.InputOperator;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// write recoverable AIN
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputNode extends Node<InputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
  private CircularBuffer<Tuple> controlTuples;
  private HashMap<String, CircularBuffer<Tuple>> afterEndWindows; // what if we did not allow user to emit control tuples.

  public InputNode(String id, InputOperator operator)
  {
    super(id, operator);
    bufferCapacity = 1024;
    controlTuples = new CircularBuffer<Tuple>(1024);
    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public final void run()
  {
    boolean inWindow = false;
    Tuple t = null;
    while (alive) {
      try {
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
                operator.beginWindow();
                break;

              case END_WINDOW:
                operator.endWindow();
                inWindow = false;
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].process(t);
                }

                handleRequests(currentWindowId);

                // i think there should be just one queue instead of one per port - lets defer till we find an example.
                for (Entry<String, CircularBuffer<Tuple>> e: afterEndWindows.entrySet()) {
                  final Sink s = outputs.get(e.getKey());
                  if (s != null) {
                    CircularBuffer<?> cb = e.getValue();
                    for (int i = cb.size(); i-- > 0;) {
                      s.process(cb.poll());
                    }
                  }
                }
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
            int oldg = generatedTupleCount;
            operator.injectTuples(currentWindowId);
            if (generatedTupleCount == oldg) {
              Thread.sleep(spinMillis);
            }
          }
          else {
            Thread.sleep(spinMillis);
          }
        }
      }
      catch (InterruptedException ex) {
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

  @Override
  public final Sink connect(String port, Sink sink)
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
      OutputPort outputPort = descriptor.outputPorts.get(port);
      outputPort.setSink(sink);

      if (outputPort == null) {
        throw new IllegalArgumentException("Unrecognized Port " + port + " for " + this);
      }

      if (sink == null) {
        outputs.remove(port);
        afterEndWindows.remove(port);
      }
      else {
        outputs.put(port, sink);
        afterEndWindows.put(port, new CircularBuffer<Tuple>(bufferCapacity));
      }

      if (sinks != Sink.NO_SINKS) {
        activateSinks();
      }
      retvalue = null;
    }

    return retvalue;
  }
}
