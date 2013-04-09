/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.client.Subscriber;
import com.malhartech.engine.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
public class BufferServerSubscriber extends Subscriber implements Stream<Object>
{
  private final HashMap<String, Sink<Object>> outputs = new HashMap<String, Sink<Object>>();
  private long baseSeconds; // needed here
  private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;
  private Sink<Object>[] emergencySinks = NO_SINKS;
  private Sink<Object>[] normalSinks = NO_SINKS;
  private StreamCodec<Object> serde;
  DataStatePair dsp = new DataStatePair();

  public BufferServerSubscriber(String id)
  {
    super(id);
  }

  @Override
  public void activate(StreamContext context)
  {
    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    baseSeconds = context.getStartingWindowId() & 0xffffffff00000000L;
    serde = context.attr(StreamContext.CODEC).get();
    activateSinks();
    activate(context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getStartingWindowId());
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    com.malhartech.bufferserver.packet.Tuple data = com.malhartech.bufferserver.packet.Tuple.getTuple(buffer, offset, size);
    Tuple t;
    switch (data.getType()) {
      case CHECKPOINT:
        serde.resetState();
        return;

      case CODEC_STATE:
        dsp.state = data.getData();
        return;

      case PAYLOAD:
        dsp.data = data.getData();
        Object o = serde.fromByteArray(dsp);
        distribute(o);
        return;

      case END_WINDOW:
        t = new EndWindowTuple();
        t.setWindowId(baseSeconds | (lastWindowId = data.getWindowId()));
        break;

      case END_STREAM:
        t = new EndStreamTuple();
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case RESET_WINDOW:
        baseSeconds = (long)data.getBaseSeconds() << 32;
        if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
          return;
        }
        t = new ResetWindowTuple();
        t.setWindowId(baseSeconds | data.getWindowWidth());
        break;

      case BEGIN_WINDOW:
        t = new Tuple(data.getType());
        t.setWindowId(baseSeconds | data.getWindowId());
        break;

      case NO_MESSAGE:
        return;

      default:
        throw new IllegalArgumentException("Unhandled Message Type " + data.getType());
    }

    distribute(t);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized void setSink(String id, Sink<Object> sink)
  {
    if (sink == null) {
      outputs.remove(id);
      if (outputs.isEmpty()) {
        sinks = NO_SINKS;
      }
    }
    else {
      outputs.put(id, (Sink)sink);
      if (sinks != NO_SINKS) {
        activateSinks();
      }
    }

  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  void activateSinks()
  {
    logger.debug("activating sinks = {} on {}", outputs);

    @SuppressWarnings("unchecked")
    Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, outputs.size());
    int i = 0;
    for (final Sink<Object> s: outputs.values()) {
      newSinks[i++] = s;
    }
    sinks = newSinks;
  }

  @Override
  public void process(Object tuple)
  {
    throw new IllegalAccessError("Attempt to pass payload " + tuple + " to " + this + " from source other than buffer server!");
  }

  @Override
  public void setup(StreamContext context)
  {
    super.setup(context.getBufferServerAddress(), context.attr(StreamContext.EVENT_LOOP).get());
  }

  @SuppressWarnings("unchecked")
  void distribute(Object o)
  {
    int i = sinks.length;
    try {
      while (i-- > 0) {
        sinks[i].process(o);
      }
    }
    catch (IllegalStateException ise) {
      suspendRead();
      if (emergencySinks.length != sinks.length) {
        emergencySinks = (Sink<Object>[])Array.newInstance(Sink.class, sinks.length);
      }
      for (int n = emergencySinks.length; n-- > 0;) {
        emergencySinks[n] = new EmergencySink();
        if (n <= i) {
          emergencySinks[n].process(o);
        }
      }
      normalSinks = sinks;
      sinks = emergencySinks;

      new Thread("EmergencyThread")
      {
        final Sink<Object>[] esinks = emergencySinks;

        @Override
        @SuppressWarnings({"NestedSynchronizedStatement", "UnusedAssignment"})
        public void run()
        {
          synchronized (BufferServerSubscriber.this) {
            boolean iterate = false;
            do {
              try {
                for (int n = esinks.length; n-- > 0;) {
                  final ArrayList<Object> list = (ArrayList<Object>)esinks[n];
                  synchronized (list) {
                    Iterator<Object> iterator = list.iterator();
                    while (iterator.hasNext()) {
                      iterate = true;
                      normalSinks[n].process(iterator.next()); /* this can throw an exception */
                      iterate = false;
                      iterator.remove();
                    }
                  }
                }
              }
              catch (IllegalStateException ise) {
              }
            }
            while (iterate);
            sinks = normalSinks;
          }

          resumeRead();
        }

      }.start();
    }
  }

  private class EmergencySink extends ArrayList<Object> implements Sink<Object>
  {
    private static final long serialVersionUID = 201304031531L;

    @Override
    public synchronized void process(Object tuple)
    {
      add(tuple);
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
}
