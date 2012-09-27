/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.annotation.PortAnnotation;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.dag.ModuleContext.ModuleRequest;
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
public abstract class AbstractInputModule extends AbstractBaseModule
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractInputModule.class);
  private transient final Tuple NO_DATA = new Tuple(DataType.NO_DATA);
  private transient CircularBuffer<Tuple> controlTuples;
  private transient HashMap<String, CircularBuffer<Tuple>> afterEndWindows; // what if we did not allow user to emit control tuples.
  private boolean alive;

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

    boolean inWindow = false;
    Tuple t = null;
    while (alive) {
      for (int size = controlTuples.size(); size-- > 0;) {
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

            context.report(generatedTupleCount, 0L, t.getWindowId());
            generatedTupleCount = 0;

            /*
             * we prefer to cater to requests at the end of the window boundary.
             */
            try {
              CircularBuffer<ModuleContext.ModuleRequest> requests = context.getRequests();
              for (int i = requests.size(); i-- > 0;) {
                requests.get().execute(this, context.getId(), t.getWindowId());
              }
            }
            catch (Exception e) {
              logger.warn("Exception while catering to external request {}", e);
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
              sinks[i].process(t);
            }
            break;
        }
      }

      if (inWindow) {
        int oldg = generatedTupleCount;
        int oldp = processedTupleCount;
        process(NO_DATA);

        if (generatedTupleCount == oldg && processedTupleCount == oldp) {
          try {
            Thread.sleep(spinMillis);
          }
          catch (InterruptedException ex) {
          }
        }
      }
    }

    logger.debug("{} sending EndOfStream", this);

    if (inWindow) {
      EndWindowTuple ewt = new EndWindowTuple();
      ewt.setWindowId(t.getWindowId());
      for (final Sink output: outputs.values()) {
        output.process(ewt);
      }
    }

    /*
     * since we are going away, we should let all the downstream operators know that.
     */
    // we need to think about this as well.
    EndStreamTuple est = new EndStreamTuple();
    if (t != null) {
      est.setWindowId(t.getWindowId());
    }
    for (final Sink output: outputs.values()) {
      output.process(est);
    }

    deactivateSinks();
  }

  @Override
  public final void deactivate()
  {
    alive = false;
  }

  @Override
  public final Sink connect(String port, Sink component)
  {
    Sink retvalue;
    if (Component.INPUT.equals(port)) {
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

  /**
   * Emit the payload to the specified output port.
   *
   * It's expected that the output port is active, otherwise NullPointerException is thrown.
   *
   * @param id
   * @param payload
   */
  public final void emit(String id, Object payload)
  {
    if (payload instanceof Tuple) {
      CircularBuffer<Tuple> cb = afterEndWindows.get(id);
      if (cb != null) {
        try {
          cb.add((Tuple)payload);
        }
        catch (BufferOverflowException boe) {
          logger.error("Emitting too many control tuples within a window, please check your logic or increase the buffer size");
        }
      }
    }
    else {
      final Sink s = outputs.get(id);
      if (s != null) {
        outputs.get(id).process(payload);
      }
    }

    generatedTupleCount++;
  }
}
