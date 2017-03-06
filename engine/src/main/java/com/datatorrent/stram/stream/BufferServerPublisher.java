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
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.client.Publisher;
import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.DataTuple;
import com.datatorrent.bufferserver.packet.EndStreamTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.WindowIdTuple;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.CustomControlTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 * Implements tuple flow of node to then buffer server in a logical stream<p>
 * <br>
 * Extends SocketOutputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a write instance of a stream and hence would take care of persistence and retaining tuples till they are consumed<br>
 * Partitioning is managed by this instance of the buffer server<br>
 * <br>
 *
 * @since 0.3.2
 */
public class BufferServerPublisher extends Publisher implements ByteCounterStream
{
  private StreamCodec<Object> serde;
  private final AtomicLong publishedByteCount;
  private EventLoop eventloop;
  private int count;
  private StatefulStreamCodec<Object> statefulSerde;

  public BufferServerPublisher(String sourceId, int queueCapacity)
  {
    super(sourceId, queueCapacity);
    this.publishedByteCount = new AtomicLong(0);
  }

  /**
   *
   * @param payload
   */
  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void put(Object payload)
  {
    count++;
    byte[] array;
    if (payload instanceof Tuple) {
      final Tuple t = (Tuple)payload;

      switch (t.getType()) {
        case CHECKPOINT:
          if (statefulSerde != null) {
            statefulSerde.resetState();
          }
          array = WindowIdTuple.getSerializedTuple((int)t.getWindowId());
          array[0] = MessageType.CHECKPOINT_VALUE;
          break;

        case BEGIN_WINDOW:
          array = BeginWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case END_WINDOW:
          array = EndWindowTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case CUSTOM_CONTROL:
          if (statefulSerde == null) {
            array = com.datatorrent.bufferserver.packet.CustomControlTuple
                .getSerializedTuple(MessageType.CUSTOM_CONTROL_VALUE, serde.toByteArray(payload));
          } else {
            DataStatePair dsp = statefulSerde.toDataStatePair(payload);
            if (dsp.state != null) {
              array = com.datatorrent.bufferserver.packet.CustomControlTuple
                  .getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
              try {
                while (!write(array)) {
                  sleep(5);
                }
              } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
              }
            }
            array = com.datatorrent.bufferserver.packet.CustomControlTuple
                .getSerializedTuple(MessageType.CUSTOM_CONTROL_VALUE, dsp.data);
          }
          break;

        case END_STREAM:
          array = EndStreamTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case RESET_WINDOW:
          com.datatorrent.stram.tuple.ResetWindowTuple rwt = (com.datatorrent.stram.tuple.ResetWindowTuple)t;
          array = ResetWindowTuple.getSerializedTuple(rwt.getBaseSeconds(), rwt.getIntervalMillis());
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    } else {
      if (statefulSerde == null) {
        array = PayloadTuple.getSerializedTuple(serde.getPartition(payload), serde.toByteArray(payload));
      } else {
        DataStatePair dsp = statefulSerde.toDataStatePair(payload);
        /*
         * if there is any state write that for the subscriber before we write the data.
         */
        if (dsp.state != null) {
          array = DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
          try {
            while (!write(array)) {
              sleep(5);
            }
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        }
        /*
         * Now that the state if any has been sent, we can proceed with the actual data we want to send.
         */
        array = PayloadTuple.getSerializedTuple(statefulSerde.getPartition(payload), dsp.data);
      }
    }

    try {
      while (!write(array)) {
        sleep(5);
      }
      publishedByteCount.addAndGet(array.length);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    put(new CustomControlTuple(payload));
    return false;
  }

  /**
   *
   * @param context
   */
  @Override
  @SuppressWarnings("unchecked")
  public void activate(StreamContext context)
  {
    setToken(context.get(StreamContext.BUFFER_SERVER_TOKEN));
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("Registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), Codec.getStringWindowId(context.getFinishedWindowId()), context.getBufferServerAddress()});
    super.activate(null, context.getFinishedWindowId());
  }

  @Override
  public void deactivate()
  {
    setToken(null);
    eventloop.disconnect(this);
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    throw new RuntimeException("OutputStream is not supposed to receive anything!");
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
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public long getByteCount(boolean reset)
  {
    if (reset) {
      return publishedByteCount.getAndSet(0);
    }

    return publishedByteCount.get();
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

  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
}
