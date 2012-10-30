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
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InputNode extends Node<InputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(InputNode.class);
  protected HashMap<String, CircularBuffer<Tuple>> afterEndWindows; // what if we did not allow user to emit control tuples.
  protected CircularBuffer<Tuple> controlTuples;
  @SuppressWarnings("VolatileArrayField")
  private volatile SyncSink[] syncsinks;
  @SuppressWarnings(value = "VolatileArrayField")
  private volatile Sink[] sinks;

  public InputNode(String id, InputOperator operator)
  {
    super(id, operator);
    controlTuples = new CircularBuffer<Tuple>(1024);
//    afterEndWindows = new HashMap<String, CircularBuffer<Tuple>>();
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
    else if (sink == null) {
      connectOutputPort(port, null);
      retvalue = null;
    }
    else {
      SyncSink ss = new SyncSink(sink, bufferCapacity);
      ss.outputPort = connectOutputPort(port, ss);
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
//                for (Entry<String, CircularBuffer<Tuple>> e: afterEndWindows.entrySet()) {
//                  final Sink s = outputs.get(e.getKey());
//                  if (s != null) {
//                    CircularBuffer<?> cb = e.getValue();
//                    for (int i = cb.size(); i-- > 0;) {
//                      s.process(cb.poll());
//                    }
//                  }
//                }
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
            boolean idling = true;
            for (int i = syncsinks.length; i-- > 0;) {
              final Iterator<?> iterator = syncsinks[i].sweep();
              if (iterator.hasNext()) {
                operator.postEmitTuples(currentWindowId, syncsinks[i].outputPort, iterator);
                idling = false;
              }
            }

            if (idling) {
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
  protected void activateSinks()
  {
    int size = outputs.size();
    if (size == 0) {
      sinks = Sink.NO_SINKS;
      syncsinks = new SyncSink[0];
    }
    else {
      CounterSink[] counterSinks = new CounterSink[size];
      SyncSink[] syncSinks = new SyncSink[size];
      for (CounterSink s: outputs.values()) {
        counterSinks[--size] = s;
        syncSinks[size] = (SyncSink)s;
      }

      sinks = counterSinks;
      syncsinks = syncSinks;
    }
  }

  @Override
  public void deactivateSinks()
  {
    for (int i = syncsinks.length; i-- > 0;) {
      operator.postEmitTuples(currentWindowId, syncsinks[i].outputPort, syncsinks[i].sweep());
    }
    syncsinks = new SyncSink[0];
    sinks = SyncSink.NO_SINKS;
  }

  class SyncSink implements CounterSink<Object>
  {
    private final Sink sink;
    private volatile Sweeper active;
    private Sweeper backup;
    private int count;
    private OutputPort<?> outputPort;

    public SyncSink(Sink sink, int buffersize)
    {
      active = new Sweeper(buffersize);
      backup = new Sweeper(buffersize);
      this.sink = sink;
    }

    @Override
    public final void process(Object tuple)
    {
      try {
        active.put(tuple);
      }
      catch (InterruptedException ex) {
        logger.warn("{} aborting emit as got interrupted while writing {}", this, tuple);
      }
    }

    public final Iterator<?> sweep()
    {
      Sweeper temp = active;
      active = backup;
      backup = temp;
      return backup.sweep();
    }

    @Override
    public int getCount()
    {
      return count;
    }

    @Override
    public int resetCount()
    {
      int ret = count;
      count = 0;
      return ret;
    }

    class Sweeper extends CircularBuffer<Object>
    {
      public Sweeper(int buffersize)
      {
        super(buffersize);
      }

      public final Iterator<?> sweep()
      {
        Iterator<?> retvalue = getFrozenIterator();
        int size = size();
        count += size;
        while (size-- > 0) {
          sink.process(pollUnsafe());
        }

        return retvalue;
      }
    }
  }
}
