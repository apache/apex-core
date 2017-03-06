/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.stream;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.client.Subscriber;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;
import com.datatorrent.stram.tuple.CheckpointTuple;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.ResetWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

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
  private final DataStatePair dsp;
  CircularBuffer<Slice> offeredFragments;
  CircularBuffer<Slice> polledFragments;
  CircularBuffer<Slice> freeFragments;
  private final ArrayDeque<CircularBuffer<Slice>> backlog;
  private int lastWindowId = WindowGenerator.MAX_WINDOW_ID;
  private final AtomicLong readByteCount;

  public BufferServerSubscriber(String id, int queueCapacity)
  {
    super(id);
    this.reservoirs = new BufferReservoir[0];
    this.reservoirMap = new HashMap<>();
    this.readByteCount = new AtomicLong(0);
    this.dsp = new DataStatePair();
    polledFragments = offeredFragments = new CircularBuffer<>(queueCapacity);
    freeFragments = new CircularBuffer<>(queueCapacity);
    backlog = new ArrayDeque<>();
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
    setToken(context.get(StreamContext.BUFFER_SERVER_TOKEN));
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("Registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), Codec.getStringWindowId(context.getFinishedWindowId()), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activate(null, context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getFinishedWindowId(), freeFragments.capacity());
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int length)
  {
    Slice f;
    if (freeFragments.isEmpty()) {
      f = new Slice(buffer, offset, length);
    } else {
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
        int newsize = offeredFragments.capacity() == MAX_SENDBUFFER_SIZE ? offeredFragments.capacity() : offeredFragments.capacity() << 1;
        backlog.add(offeredFragments = new CircularBuffer<>(newsize));
        offeredFragments.add(f);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setup(StreamContext context)
  {
    StreamCodec<?> codec = context.get(StreamContext.CODEC);
    if (codec == null) {
      statefulSerde = ((StatefulStreamCodec<Object>)StreamContext.CODEC.defaultValue).newInstance();
    } else if (codec instanceof StatefulStreamCodec) {
      statefulSerde = ((StatefulStreamCodec<Object>)codec).newInstance();
    } else {
      serde = (StreamCodec<Object>)codec;
    }
    baseSeconds = context.getFinishedWindowId() & 0xffffffff00000000L;
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
    setToken(null);
  }

  @Override
  public void teardown()
  {
  }

  @SuppressWarnings("VolatileArrayField")
  private volatile BufferReservoir[] reservoirs;
  private final HashMap<String, BufferReservoir> reservoirMap;

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

  public SweepableReservoir acquireReservoirForPersistStream(String id, int capacity, StreamCodec<?> streamCodec)
  {
    BufferReservoir r = reservoirMap.get(id);
    if (r == null) {
      reservoirMap.put(id, r = new BufferReservoirForPersistStream(capacity, (StreamCodecWrapperForPersistance<Object>)streamCodec));
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

  @Override
  public boolean putControl(ControlTuple payload)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public SweepableReservoir releaseReservoir(String sinkId)
  {
    BufferReservoir r = reservoirMap.remove(sinkId);
    if (r != null) {
      BufferReservoir[] newReservoirs = new BufferReservoir[reservoirs.length - 1];

      int j = 0;
      for (BufferReservoir reservoir: reservoirs) {
        if (reservoir != r) {
          newReservoirs[j++] = reservoir;
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
    protected boolean skipObject = false;
    private Sink<Object> sink;
    int count;

    BufferReservoir(int capacity)
    {
      super(capacity);
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      int size = size();
      if (dataTupleAware) {
        Iterator<Object> iterator = getFrozenIterator();
        while (iterator.hasNext()) {
          if (iterator.next() instanceof Tuple) {
            size--;
          }
        }
      }
      return size;
    }

    @Override
    public Sink<Object> setSink(Sink<Object> sink)
    {
      try {
        return this.sink;
      } finally {
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
              o = processPayload(data);
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

            case CUSTOM_CONTROL:
              o = processPayload(data);
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
          if (skipObject) {
            skipObject = false;
          } else {
            for (int i = reservoirs.length; i-- > 0;) {
              reservoirs[i].add(o);
            }
          }
        }
      }

      return null;
    }

    protected Object processPayload(com.datatorrent.bufferserver.packet.Tuple data)
    {
      Object o;
      if (statefulSerde == null) {
        o = serde.fromByteArray(data.getData());
      } else {
        dsp.data = data.getData();
        o = statefulSerde.fromDataStatePair(dsp);
      }
      return o;
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
        return count;
      } finally {
        if (reset) {
          count = 0;
        }
      }
    }

  }

  public class BufferReservoirForPersistStream extends BufferReservoir
  {
    StreamCodecWrapperForPersistance wrapperStreamCodec;

    BufferReservoirForPersistStream(int capacity, StreamCodecWrapperForPersistance<Object> streamCodec)
    {
      super(capacity);
      wrapperStreamCodec = streamCodec;
    }

    @Override
    protected Object processPayload(com.datatorrent.bufferserver.packet.Tuple data)
    {
      Object o = wrapperStreamCodec.fromByteArray(data.getData());
      if (!wrapperStreamCodec.shouldCaptureEvent(o)) {
        skipObject = true;
      }

      return o;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
}
