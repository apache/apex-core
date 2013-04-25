/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.api.StreamCodec.DataStatePair;
import com.malhartech.bufferserver.client.Subscriber;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.engine.SweepableReservoir;
import com.malhartech.engine.WindowGenerator;
import com.malhartech.netlet.Client.Fragment;
import com.malhartech.netlet.EventLoop;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.tuple.Tuple;
import com.malhartech.util.CircularBuffer;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
public class BufferServerSubscriber extends Subscriber implements Stream
{
  private boolean suspended;
  private long baseSeconds;
  protected StreamCodec<Object> serde;
  private EventLoop eventloop;
  private DataStatePair dsp = new DataStatePair();
  CircularBuffer<Fragment> offeredFragments;
  CircularBuffer<Fragment> polledFragments;
  CircularBuffer<Fragment> freeFragments;
  private final ArrayDeque<CircularBuffer<Fragment>> backlog;
  private int lastWindowId;
  private long readByteCount = 0;

  @SuppressWarnings("unchecked")
  public BufferServerSubscriber(String id, int queueCapacity)
  {
    super(id);
    polledFragments = offeredFragments = new CircularBuffer<Fragment>(queueCapacity);
    freeFragments = new CircularBuffer<Fragment>(queueCapacity);
    backlog = new ArrayDeque<CircularBuffer<Fragment>>();
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.attr(StreamContext.EVENT_LOOP).get();
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activate(context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getStartingWindowId());
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int length)
  {
    Fragment f;
    if (freeFragments.isEmpty()) {
      f = new Fragment(buffer, offset, length);
    }
    else {
      f = freeFragments.pollUnsafe();
      f.buffer = buffer;
      f.offset = offset;
      f.length = length;
    }

    if (!offeredFragments.offer(f)) {
      synchronized (backlog) {
        if (!suspended) {
          suspendRead();
          suspended = true;
        }
        int newsize = offeredFragments.capacity() == 32 * 1024 ? offeredFragments.capacity() : offeredFragments.capacity() << 1;
        backlog.add(offeredFragments = new CircularBuffer<Fragment>(newsize));
        offeredFragments.add(f);
      }
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  @Override
  public void setup(StreamContext context)
  {
    serde = context.attr(StreamContext.CODEC).get();
    baseSeconds = context.getStartingWindowId() & 0xffffffff00000000L;
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

  @SuppressWarnings("VolatileArrayField")
  private volatile BufferReservoir[] reservoirs = new BufferReservoir[0];
  private HashMap<String, BufferReservoir> reservoirMap = new HashMap<String, BufferReservoir>();

  public SweepableReservoir acquireReservoir(String id, int capacity)
  {
    BufferReservoir r = reservoirMap.get(id);
    if (r == null) {
      reservoirMap.put(id, r = new BufferReservoir(capacity));
      BufferReservoir[] newReservoirs = new BufferReservoir[reservoirs.length + 1];
      newReservoirs[reservoirs.length] = r;
      for (int i = reservoirs.length; i-- > 0;) {
        newReservoirs[i] = reservoirs[i];
      }
      reservoirs = newReservoirs;
    }

    return r;
  }

  @Override
  public void process(Object tuple)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public SweepableReservoir releaseReservoir(String sinkId)
  {
    BufferReservoir r = reservoirMap.remove(sinkId);
    if (r != null) {
      BufferReservoir[] newReservoirs = new BufferReservoir[reservoirs.length - 1];

      int j = 0;
      for (int i = 0; i < reservoirs.length; i++) {
        if (reservoirs[i] != r) {
          newReservoirs[j++] = reservoirs[i];
        }
      }

      reservoirs = newReservoirs;
    }

    return r;
  }

  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   * @return the readByteCount
   */
  public long resetReadByteCount()
  {
    try {
      return readByteCount;
    }
    finally {
      readByteCount = 0;
    }
  }

  class BufferReservoir extends CircularBuffer<Object> implements SweepableReservoir
  {
    private Sink<Object> sink;
    int count;

    BufferReservoir(int capacity)
    {
      super(capacity);
    }

    @Override
    public void setSink(Sink<Object> sink)
    {
      this.sink = sink;
    }

    @Override
    public Tuple sweep()
    {
      final int size = size();
      if (size > 0) {
        for (int i = 1; i <= size; i++) {
          if (peekUnsafe() instanceof Tuple) {
            count += i;
            return (Tuple)peekUnsafe();
          }
          sink.process(pollUnsafe());
        }

        count += size;
      }

      synchronized (backlog) {
        /* find out the minimum remaining capacity in all the other buffers and consume those many tuples from bufferserver */
        int min = polledFragments.size();
        if (min == 0) {
          if (offeredFragments == polledFragments) {
            if (suspended) {
              resumeRead();
              suspended = false;
            }
            return null;
          }
          polledFragments = backlog.remove();
          min = polledFragments.size();
        }

        for (int i = reservoirs.length; i-- > 0;) {
          if (reservoirs[i].remainingCapacity() < min) {
            min = reservoirs[i].remainingCapacity();
          }
        }

        while (min-- > 0) {
          Fragment fm = polledFragments.pollUnsafe();
          com.malhartech.bufferserver.packet.Tuple data = com.malhartech.bufferserver.packet.Tuple.getTuple(fm.buffer, fm.offset, fm.length);
          Object o;
          switch (data.getType()) {
            case NO_MESSAGE:
              freeFragments.add(fm);
              continue;

            case CHECKPOINT:
              serde.resetState();
              freeFragments.add(fm);
              continue;

            case CODEC_STATE:
              dsp.state = data.getData();
              freeFragments.add(fm);
              continue;

            case PAYLOAD:
              dsp.data = data.getData();
              o = serde.fromByteArray(dsp);
              break;

            case END_WINDOW:
              //logger.debug("received {}", data);
              o = new EndWindowTuple(baseSeconds | (lastWindowId = data.getWindowId()));
              break;

            case END_STREAM:
              o = new EndStreamTuple(baseSeconds | data.getWindowId());
              break;

            case RESET_WINDOW:
              baseSeconds = (long)data.getBaseSeconds() << 32;
              if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
                continue;
              }
              o = new ResetWindowTuple(baseSeconds | data.getWindowWidth());
              break;

            case BEGIN_WINDOW:
              //logger.debug("received {}", data);
              o = new Tuple(data.getType(), baseSeconds | data.getWindowId());
              break;

            default:
              throw new IllegalArgumentException("Unhandled Message Type " + data.getType());
          }

          freeFragments.offer(fm);
          for (int i = reservoirs.length; i-- > 0;) {
            reservoirs[i].add(o);
          }
        }
      }

      return null;
    }

    @Override
    public int resetCount()
    {
      int retvalue = count;
      count = 0;
      return retvalue;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
}
