/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.engine.DefaultStreamCodec;
import com.malhartech.engine.SweepableReservoir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import junit.framework.Assert;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FastPublisherTest
{
  public FastPublisherTest()
  {
  }

  @Test
  public void testSerialization() throws Exception
  {
    FastPublisherImpl publisher = new FastPublisherImpl(2);
    String message= "hello!";
    publisher.put(message);
    byte[] buffer = publisher.consume();

    FastSubscriber subscriber = new FastSubscriber("subscriber", 1024);
    subscriber.serde = new DefaultStreamCodec<Object>();
    SweepableReservoir sr = subscriber.acquireReservoir("res", 1024);
    sr.setSink(new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        logger.debug("{}", tuple);
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
    assert(buffer.length == (size + 2) * 1024);

    int index = 0;
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
      super(buffercount);
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
          }
          else {
            index++;
          }
        }
        buffer = readBuffers[index];
      }
      while (true);

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
          }
          else {
            readIndex++;
          }
        }
        readBuffer = readBuffers[readIndex];
      }
      while (true);

      return retbuffer;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(FastPublisherTest.class);
}
