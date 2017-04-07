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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.SweepableReservoir;

/**
 *
 */
@Ignore // ignored since they do not belong here!
public class FastPublisherTest
{
  public FastPublisherTest()
  {
  }

  @Test
  public void testSerialization() throws Exception
  {
    FastPublisherImpl publisher = new FastPublisherImpl(24 * 1024);
    final String message = "hello!";
    publisher.put(message);
    byte[] buffer = publisher.consume();

    FastSubscriber subscriber = new FastSubscriber("subscriber", 1024);
    subscriber.serde = subscriber.statefulSerde = new DefaultStatefulStreamCodec<>();
    SweepableReservoir sr = subscriber.acquireReservoir("res", 1024);
    sr.setSink(new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        assert (tuple.equals(message));
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    });

    int size = buffer[0];
    size |= buffer[1] << 8;
    Assert.assertEquals("size", buffer.length - 2, size);
    subscriber.onMessage(buffer, 2, buffer.length - 2);
    sr.sweep();
    sr.sweep();

    for (int i = 0; i < 1024; i++) {
      publisher.put(message);
    }

    buffer = publisher.consume();
    assert (buffer.length == (size + 2) * 1024);

    int index = 0;
    for (int i = 0; i < 1024; i++) {
      size = buffer[index++];
      size |= buffer[index++] << 8;
      subscriber.onMessage(buffer, index, size);
      index += size;
    }

    sr.sweep();
    sr.sweep();

    for (int i = 0; i < 1024; i++) {
      publisher.put(message);
    }

    buffer = publisher.consume();
    assert (buffer.length == (size + 2) * 1024);

    index = 0;
    for (int i = 0; i < 1024; i++) {
      size = buffer[index++];
      size |= buffer[index++] << 8;
      subscriber.onMessage(buffer, index, size);
      index += size;
    }

    sr.sweep();
    sr.sweep();

  }

  static class FastPublisherImpl extends FastPublisher
  {
    FastPublisherImpl(int buffercount)
    {
      super("testpublisher", buffercount);
    }

    @Override
    public void write() throws IOException
    {
      logger.debug("disabled intentionally - please use consume instead");
    }

    public byte[] consume()
    {
      int size = 0;

      int index = readIndex;
      ByteBuffer buffer = readBuffer;
      do {
        synchronized (buffer) {
          size += buffer.remaining();
          if (buffer.position() + buffer.remaining() < buffer.capacity()) {
            break;
          }
          if (index == lastIndex) {
            index = 0;
          } else {
            index++;
          }
        }
        buffer = readBuffers[index];
      } while (true);

      byte[] retbuffer = new byte[size];

      index = 0;
      do {
        synchronized (readBuffer) {
          int remaining = readBuffer.remaining();
          readBuffer.get(retbuffer, index, remaining);
          index += remaining;
          if (readBuffer.position() < readBuffer.capacity()) {
            break;
          }
          if (readIndex == lastIndex) {
            readIndex = 0;
          } else {
            readIndex++;
          }
        }
        readBuffer = readBuffers[readIndex];
      } while (true);

      return retbuffer;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(FastPublisherTest.class);
}
