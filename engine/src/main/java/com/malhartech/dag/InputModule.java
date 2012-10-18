/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.PortAnnotation;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.util.CircularBuffer;
import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// write recoverable AIN
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class InputModule extends BaseModule
{
  private static final Logger logger = LoggerFactory.getLogger(InputModule.class);
  private transient final Tuple NO_DATA = new Tuple(DataType.NO_DATA);
  private transient CircularBuffer<Tuple> controlTuples;
  private transient HashMap<String, CircularBuffer<Tuple>> afterEndWindows; // what if we did not allow user to emit control tuples.

  public InputModule()
  {
    controlTuples = new CircularBuffer<Tuple>(1024);
    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public final void activate(OperatorContext context)
  {
    activateSinks();
    alive = true;
    activated(context);

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
                NO_DATA.setWindowId(t.getWindowId());
                beginWindow();
                break;

              case END_WINDOW:
                endWindow();
                inWindow = false;
                for (int i = sinks.length; i-- > 0;) {
                  sinks[i].process(t);
                }

                /*
                 * we prefer to cater to requests at the end of the window boundary.
                 */
                try {
                  CircularBuffer<OperatorContext.ModuleRequest> requests = context.getRequests();
                  for (int i = requests.size(); i-- > 0;) {
                    //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
                    requests.remove().execute(this, context.getId(), t.getWindowId());
                  }
                }
                catch (Exception e) {
                  logger.warn("Exception while catering to external request {}", e);
                }

                context.report(generatedTupleCount, 0L, t.getWindowId());
                generatedTupleCount = 0;

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
            process(NO_DATA);
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

    deactivated(context);
    emitEndStream();
    deactivateSinks();
  }

  @Override
  public final Sink connect(String port, Sink component)
  {
    Sink retvalue;
    if (Component.INPUT.equals(port)) {
      port = Component.INPUT;
      retvalue = new Sink()
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
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
      PortAnnotation pa = getPort(port);
      if (pa == null) {
        throw new IllegalArgumentException("Unrecognized Port " + port + " for " + this);
      }

      port = pa.name();
      if (component == null) {
        outputs.remove(port);
        afterEndWindows.remove(port);
      }
      else {
        outputs.put(port, component);
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
}
