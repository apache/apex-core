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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// write recoverable AIN
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractInputModule extends AbstractBaseModule
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputModule.class);
  private transient final Tuple NO_DATA = new Tuple(DataType.NO_DATA);
  private transient CircularBuffer<Tuple> controlTuples;
  private transient HashMap<String, CircularBuffer<Tuple>> afterEndWindows; // what if we did not allow user to emit control tuples.

  public AbstractInputModule()
  {
    controlTuples = new CircularBuffer<Tuple>(1024);
    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();
  }

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public final void activate(ModuleContext context)
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
            t = controlTuples.get();
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
                  CircularBuffer<ModuleContext.ModuleRequest> requests = context.getRequests();
                  for (int i = requests.size(); i-- > 0;) {
                    //logger.debug("endwindow: " + t.getWindowId() + " lastprocessed: " + context.getLastProcessedWindowId());
                    requests.get().execute(this, context.getId(), t.getWindowId());
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
                      s.process(cb.get());
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
          while (true) {
            try {
              controlTuples.add((Tuple)payload);
              break;
            }
            catch (BufferOverflowException boe) {
              try {
                Thread.sleep(spinMillis);
              }
              catch (InterruptedException ex) {
                break;
              }
            }
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
