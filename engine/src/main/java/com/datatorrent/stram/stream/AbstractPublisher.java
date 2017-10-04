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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.DataTuple;
import com.datatorrent.bufferserver.packet.EndStreamTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.WindowIdTuple;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.StatefulStreamCodec;
import com.datatorrent.stram.engine.ByteCounterStream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.CustomControlTuple;
import com.datatorrent.stram.tuple.Tuple;

public abstract class AbstractPublisher implements ByteCounterStream
{
  private StreamCodec<Object> serde;
  private final AtomicLong publishedByteCount;
  private int count;
  private StatefulStreamCodec<Object> statefulSerde;

  public AbstractPublisher()
  {
    this.publishedByteCount = new AtomicLong(0);
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
  public void teardown()
  {

  }

  @Override
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

        case END_STREAM:
          array = EndStreamTuple.getSerializedTuple((int)t.getWindowId());
          break;

        case RESET_WINDOW:
          com.datatorrent.stram.tuple.ResetWindowTuple rwt = (com.datatorrent.stram.tuple.ResetWindowTuple)t;
          array = ResetWindowTuple.getSerializedTuple(rwt.getBaseSeconds(), rwt.getIntervalMillis());
          break;

        case CUSTOM_CONTROL:
          if (statefulSerde == null) {
            array = com.datatorrent.bufferserver.packet.CustomControlTuple
                .getSerializedTuple(MessageType.CUSTOM_CONTROL_VALUE, serde.toByteArray(payload));
          } else {
            StatefulStreamCodec.DataStatePair dsp = statefulSerde.toDataStatePair(payload);
            if (dsp.state != null) {
              array = com.datatorrent.bufferserver.packet.CustomControlTuple
                  .getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
              send(array);
            }
            array = com.datatorrent.bufferserver.packet.CustomControlTuple
                .getSerializedTuple(MessageType.CUSTOM_CONTROL_VALUE, dsp.data);
          }
          break;

        default:
          throw new UnsupportedOperationException("this data type is not handled in the stream");
      }
    } else {
      if (statefulSerde == null) {

        Slice slice = serde.toByteArray(payload);
        int partition = serde.getPartition(payload);

        array = PayloadTuple.getSerializedTuple(partition, slice);
      } else {
        StatefulStreamCodec.DataStatePair dsp = statefulSerde.toDataStatePair(payload);
        /*
         * if there is any state write that for the subscriber before we write the data.
         */
        if (dsp.state != null) {
          array = DataTuple.getSerializedTuple(MessageType.CODEC_STATE_VALUE, dsp.state);
          send(array);
        }
        /*
         * Now that the state if any has been sent, we can proceed with the actual data we want to send.
         */

        array = PayloadTuple.getSerializedTuple(statefulSerde.getPartition(payload), dsp.data);
      }
    }

    send(array);
    publishedByteCount.addAndGet(array.length);
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
  public void deactivate()
  {
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    put(new CustomControlTuple(payload));
    return false;
  }

  public abstract void send(byte[] data);

  private static final Logger logger = LoggerFactory.getLogger(AbstractPublisher.class);
}
