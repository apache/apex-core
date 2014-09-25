/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.stream;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;

import com.datatorrent.bufferserver.client.Subscriber;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.Slice;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.tuple.*;

/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 *
 * @since 0.3.2
 */
public class BufferServerSubscriber extends Subscriber implements ByteCounterStream
{
  private boolean suspended;
  private long baseSeconds;
  protected StreamCodec<Object> serde;
  protected StatefulStreamCodec<Object> statefulSerde;
  protected EventLoop eventloop;
  private DataStatePair dsp = new DataStatePair();
  CircularBuffer<Slice> offeredFragments;
  CircularBuffer<Slice> polledFragments;
  CircularBuffer<Slice> freeFragments;
  private final ArrayDeque<CircularBuffer<Slice>> backlog;
  private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;
  private AtomicLong readByteCount = new AtomicLong(0);

  public BufferServerSubscriber(String id, int queueCapacity)
  {
    super(id);
    polledFragments = offeredFragments = new CircularBuffer<Slice>(queueCapacity);
    freeFragments = new CircularBuffer<Slice>(queueCapacity);
    backlog = new ArrayDeque<CircularBuffer<Slice>>();
  }

  @Override
  public void read(int len)
  {
    super.read(len);
    readByteCount.addAndGet(len);
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("Registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), Codec.getStringWindowId(context.getFinishedWindowId()), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activate(null, context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getFinishedWindowId());
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int length)
  {
    Slice f;
    if (freeFragments.isEmpty()) {
      f = new Slice(buffer, offset, length);
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
        backlog.add(offeredFragments = new CircularBuffer<Slice>(newsize));
        offeredFragments.add(f);
      }
    }
  }

  @Override
  public void setup(StreamContext context)
  {
    serde = context.get(StreamContext.CODEC);
    statefulSerde = serde instanceof StatefulStreamCodec ? (StatefulStreamCodec<Object>)serde : null;
    baseSeconds = context.getFinishedWindowId() & 0xffffffff00000000L;
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
  public void put(Object tuple)
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
  public int getCount(boolean reset)
  {
    return 0;
  }

  @Override
  public long getByteCount(boolean reset)
  {
    if (reset) {
      return readByteCount.getAndSet(0);
    }

    return readByteCount.get();
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
    public Sink<Object> setSink(Sink<Object> sink)
    {
      try {
        return this.sink;
      }
      finally {
        this.sink = sink;
      }
    }

    @Override
    public Tuple sweep()
    {
      final int size = size();
      if (size > 0) {
        for (int i = 0; i < size; i++) {
          if (peekUnsafe() instanceof Tuple) {
            count += i;
            return (Tuple)peekUnsafe();
          }
          sink.put(pollUnsafe());
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
          Slice fm = polledFragments.pollUnsafe();
          com.datatorrent.bufferserver.packet.Tuple data = com.datatorrent.bufferserver.packet.Tuple.getTuple(fm.buffer, fm.offset, fm.length);
          Object o;
          switch (data.getType()) {
            case NO_MESSAGE:
              freeFragments.offer(fm);
              continue;

            case CODEC_STATE:
              dsp.state = data.getData();
              freeFragments.offer(fm);
              continue;

            case RESET_WINDOW:
              baseSeconds = (long)data.getBaseSeconds() << 32;
              if (lastWindowId < WindowGenerator.MAX_WINDOW_ID) {
                freeFragments.offer(fm);
                continue;
              }
              o = new ResetWindowTuple(baseSeconds | data.getWindowWidth());
              break;

            case PAYLOAD:
              if (statefulSerde == null) {
                o = serde.fromByteArray(data.getData());
              }
              else {
                dsp.data = data.getData();
                o = statefulSerde.fromDataStatePair(dsp);
              }
              break;

            case CHECKPOINT:
              if (statefulSerde != null) {
                statefulSerde.resetState();
              }
              o = new CheckpointTuple(baseSeconds | data.getWindowId());
              break;

            case END_WINDOW:
              //logger.debug("received {}", data);
              o = new EndWindowTuple(baseSeconds | (lastWindowId = data.getWindowId()));
              break;

            case END_STREAM:
              o = new EndStreamTuple(baseSeconds | data.getWindowId());
              break;

            case BEGIN_WINDOW:
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
    public int getCount(boolean reset)
    {
      try {
        return count;
      }
      finally {
        if (reset) {
          count = 0;
        }
      }
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
}
