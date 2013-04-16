/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.bufferserver.client.Subscriber;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.netlet.EventLoop;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
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
  private long baseSeconds;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;
  private Sink<Object>[] emergencySinks = NO_SINKS;
  private Sink<Object>[] normalSinks = NO_SINKS;
  private StreamCodec<Object> serde;
  private EventLoop eventloop;

  public BufferServerSubscriber(String id)
  {
    super(id);
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.attr(StreamContext.EVENT_LOOP).get();
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activateSinks();
    activate(context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getStartingWindowId());
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    Fragment f = new Fragment(buffer, offset, size);
    distribute(f);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSink(String id, Sink<Object> sink)
  {
    if (sink == null) {
      outputs.remove(id);
//      if (outputs.isEmpty()) {
//        sinks = NO_SINKS;
//      }
    }
    else {
      outputs.put(id, (Sink)sink);
//      if (sinks != NO_SINKS) {
//        activateSinks();
//      }
    }

  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  void activateSinks()
  {
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
    serde = context.attr(StreamContext.CODEC).get();
    baseSeconds = context.getStartingWindowId() & 0xffffffff00000000L;
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
    }
  }

  @Override
  public void endMessage()
  {
    if (sinks == emergencySinks) {
      new Thread("EmergencyThread")
      {
        final Sink<Object>[] esinks = emergencySinks;

        @Override
        @SuppressWarnings("UnusedAssignment")
        public void run()
        {
          boolean iterate = false;
          do {
            try {
              for (int n = esinks.length; n-- > 0;) {
                @SuppressWarnings("unchecked")
                final ArrayList<Object> list = (ArrayList<Object>)esinks[n];
                Iterator<Object> iterator = list.iterator();
                while (iterator.hasNext()) {
                  iterate = true;
                  normalSinks[n].process(iterator.next()); /* this can throw an exception */
                  iterate = false;
                  iterator.remove();
                }
              }
            }
            catch (IllegalStateException ise) {
            }
          }
          while (iterate);
          sinks = normalSinks;

          resumeRead();
        }

      }.start();
    }
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
  }

  @Override
  public void teardown()
  {
  }

  /**
   * @return the serde
   */
  public StreamCodec<Object> getSerde()
  {
    return serde;
  }

  /**
   * @return the baseSeconds
   */
  public long getBaseSeconds()
  {
    return baseSeconds;
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
