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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndStreamTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.packet.WindowIdTuple;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.netlet.Listener;
import com.datatorrent.netlet.Listener.ClientListener;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.CustomControlTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 * <p>FastPublisher class.</p>
 *
 * TODO:- Implement token security
 *
 * @since 0.3.2
 */
public class FastPublisher extends Kryo implements ClientListener, Stream
{
  public static final int BUFFER_CAPACITY = 8 * 1024;
  private SelectionKey key;
  private EventLoop eventloop;
  private int count;
  private long spinMillis;
  protected final int lastIndex;
  protected final ByteBuffer[] readBuffers;
  protected ByteBuffer readBuffer;
  protected volatile int readIndex;
  private final ByteBuffer[] writeBuffers;
  private ByteBuffer writeBuffer;
  private int writeIndex;
  private final String id;
  private boolean write = true;

  public FastPublisher(String id, int streamingWindowThroughput)
  {
    this.id = id;

    int countOf8kBuffers = streamingWindowThroughput / (8 * 1024);
    if (streamingWindowThroughput % (8 * 1024) != 0) {
      countOf8kBuffers++;
    }
    if (countOf8kBuffers < 2) {
      countOf8kBuffers = 2;
    }

    writeBuffers = new ByteBuffer[countOf8kBuffers];
    readBuffers = new ByteBuffer[countOf8kBuffers];
    for (int i = countOf8kBuffers; i-- > 0;) {
      writeBuffers[i] = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
      writeBuffers[i].order(ByteOrder.LITTLE_ENDIAN);
      readBuffers[i] = writeBuffers[i].asReadOnlyBuffer();
      readBuffers[i].limit(0);
    }

    writeBuffer = writeBuffers[0];
    readBuffer = readBuffers[0];
    lastIndex = countOf8kBuffers - 1;
  }

  @Override
  public void read() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int read;
    if ((read = channel.read(ByteBuffer.allocate(1))) > 0) {
      throw new RuntimeException("Publisher " + this + " is not supposed to receive any data");
    } else if (read == -1) {
      try {
        channel.close();
      } finally {
        unregistered(key);
        key.attach(Listener.NOOP_CLIENT_LISTENER);
      }
    } else {
      logger.debug("{} read 0 bytes", this);
    }
  }

  @Override
  public void write() throws IOException
  {
    final ByteBuffer lReadBuffer = readBuffer;
    SocketChannel sc = (SocketChannel)key.channel();
    do {
      synchronized (lReadBuffer) {
        sc.write(readBuffer);
        if (readBuffer.position() < readBuffer.capacity()) {
          if (!readBuffer.hasRemaining()) {
            synchronized (readBuffers) {
              if (write) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                write = false;
              }
            }
          }
          return;
        }
        readBuffer.limit(0);
        if (readIndex == lastIndex) {
          readIndex = 0;
        } else {
          readIndex++;
        }
      }
      readBuffer = readBuffers[readIndex];
    } while (true);
  }

  @Override
  public void handleException(Exception cce, EventLoop el)
  {
    logger.debug("Generically handling", cce);
  }

  @Override
  public void registered(SelectionKey key)
  {
    this.key = key;
  }

  @Override
  public void unregistered(SelectionKey key)
  {
    // do something so that no more data can be written to this channel. But the data already written will be sent.
  }

  @Override
  public void setup(StreamContext context)
  {
    spinMillis = 5; // somehow get it context.getValue(PortContext.SPIN_MILLIS, 5);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), context.getFinishedWindowId(), context.getBufferServerAddress()});
    byte[] serializedRequest = PublishRequestTuple.getSerializedRequest(com.datatorrent.bufferserver.packet.Tuple.FAST_VERSION, id, context.getFinishedWindowId());
    assert (serializedRequest.length < 128);
    writeBuffers[0].put((byte)serializedRequest.length);
    writeBuffers[0].put(serializedRequest);
    synchronized (readBuffers) {
      readBuffers[0].limit(writeBuffers[0].position());
      if (!write) {
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        write = true;
        key.selector().wakeup();
      }
    }
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
  }

  long item;

  @Override
  @SuppressWarnings("SleepWhileInLoop")
  public void put(Object tuple)
  {
    if (tuple instanceof Tuple) {
      final Tuple t = (Tuple)tuple;

      byte[] array;
      switch (t.getType()) {
        case CHECKPOINT:
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
          array = null;
          // TODO implement
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

      int size = array.length;
      if (writeBuffer.hasRemaining()) {
        writeBuffer.put((byte)size);
        if (writeBuffer.hasRemaining()) {
          writeBuffer.put((byte)(size >> 8));
        } else {
          synchronized (readBuffers) {
            readBuffers[writeIndex].limit(BUFFER_CAPACITY);
          }
          advanceWriteBuffer();
          writeBuffer.put((byte)(size >> 8));
        }
      } else {
        synchronized (readBuffers) {
          readBuffers[writeIndex].limit(BUFFER_CAPACITY);
        }
        advanceWriteBuffer();
        writeBuffer.put((byte)size);
        writeBuffer.put((byte)(size >> 8));
      }

      int remaining = writeBuffer.remaining();
      if (remaining < size) {
        int offset = 0;
        do {
          writeBuffer.put(array, offset, remaining);
          offset += remaining;
          size -= remaining;
          synchronized (readBuffers) {
            readBuffers[writeIndex].limit(BUFFER_CAPACITY);
          }
          advanceWriteBuffer();
          remaining = writeBuffer.remaining();
          if (size <= remaining) {
            writeBuffer.put(array, offset, size);
            break;
          }
        } while (true);
      } else {
        writeBuffer.put(array);
      }
      synchronized (readBuffers) {
        readBuffers[writeIndex].limit(writeBuffer.position());
      }
    } else {
      count++;
      int hashcode = tuple.hashCode();

      int wi = writeIndex;
      int position = writeBuffer.position();

      int newPosition = position + 2 /* for short size */ + 1 /* for data type */ + 4 /* for partition */;
      if (newPosition > BUFFER_CAPACITY) {
        writeBuffer.position(BUFFER_CAPACITY);
        advanceWriteBuffer();
        writeBuffer.position(newPosition - BUFFER_CAPACITY);
      } else {
        writeBuffer.position(newPosition);
      }

      writeClassAndObject(output, tuple);
      int size;
      if (wi == writeIndex) {
        size = writeBuffer.position() - position - 2 /* for short size */;
        assert (size <= Short.MAX_VALUE);
        writeBuffer.put(position++, (byte)size);
        writeBuffer.put(position++, (byte)(size >> 8));
        writeBuffer.put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
        writeBuffer.put(position++, (byte)hashcode);
        writeBuffer.put(position++, (byte)(hashcode >> 8));
        writeBuffer.put(position++, (byte)(hashcode >> 16));
        writeBuffer.put(position, (byte)(hashcode >> 24));
        synchronized (readBuffers[wi]) {
          readBuffers[wi].limit(writeBuffer.position());
        }
      } else {
        size = BUFFER_CAPACITY - position - 2 + writeBuffer.position();
        int index = writeIndex;
        synchronized (readBuffers[index]) {
          readBuffers[index].position(0);
          readBuffers[index].limit(writeBuffer.position());
        }
        do {
          if (index == 0) {
            index = lastIndex;
          } else {
            index--;
          }

          if (index == wi) {
            break;
          }
          synchronized (readBuffers[index]) {
            readBuffers[index].position(0);
            readBuffers[index].limit(BUFFER_CAPACITY);
          }
          size += BUFFER_CAPACITY;
        } while (true);
        assert (size <= Short.MAX_VALUE);
        index = wi;
        switch (position) {
          case BUFFER_CAPACITY:
            position = 0;
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 1:
            writeBuffers[wi].put(position, (byte)size);
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 2:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position, (byte)(size >> 8));
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 3:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 4:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position, (byte)hashcode);
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 5:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position, (byte)(hashcode >> 8));
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          case BUFFER_CAPACITY - 6:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position, (byte)(hashcode >> 16));
            if (wi == lastIndex) {
              wi = 0;
            } else {
              wi++;
            }
            position = 0;
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;

          default:
            writeBuffers[wi].put(position++, (byte)size);
            writeBuffers[wi].put(position++, (byte)(size >> 8));
            writeBuffers[wi].put(position++, com.datatorrent.bufferserver.packet.MessageType.PAYLOAD_VALUE);
            writeBuffers[wi].put(position++, (byte)hashcode);
            writeBuffers[wi].put(position++, (byte)(hashcode >> 8));
            writeBuffers[wi].put(position++, (byte)(hashcode >> 16));
            writeBuffers[wi].put(position, (byte)(hashcode >> 24));
            break;
        }
        synchronized (readBuffers[index]) {
          readBuffers[index].limit(BUFFER_CAPACITY);
        }
      }
    }

    if (!write) {
      key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
      write = true;
      key.selector().wakeup();
    }
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    put(new CustomControlTuple(payload));
    return true;
  }

  @SuppressWarnings("SleepWhileInLoop")
  public void advanceWriteBuffer()
  {
    if (writeIndex == lastIndex) {
      writeIndex = 0;
    } else {
      writeIndex++;
    }

    try {
      while (writeIndex == readIndex) {
        sleep(spinMillis);
      }

      writeBuffer = writeBuffers[writeIndex];
      writeBuffer.clear();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    if (reset) {
      try {
        return count;
      } finally {
        count = 0;
      }
    }

    return count;
  }

  private final Output output = new Output()
  {
    @Override
    public void write(int value) throws KryoException
    {
      if (!writeBuffer.hasRemaining()) {
        advanceWriteBuffer();
      }
      writeBuffer.put((byte)value);
    }

    @Override
    public void write(byte[] bytes) throws KryoException
    {
      int remaining = writeBuffer.remaining();
      if (bytes.length > remaining) {
        writeBuffer.put(bytes, 0, remaining);
        advanceWriteBuffer();
        write(bytes, remaining, bytes.length - remaining);
      } else {
        writeBuffer.put(bytes);
      }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws KryoException
    {
      int remaining = writeBuffer.remaining();
      while (length > remaining) {
        writeBuffer.put(bytes, offset, remaining);
        offset += remaining;
        length -= remaining;
        advanceWriteBuffer();
        remaining = writeBuffer.remaining();
      }

      writeBuffer.put(bytes, offset, length);
    }

    @Override
    public void writeByte(byte value) throws KryoException
    {
      if (!writeBuffer.hasRemaining()) {
        advanceWriteBuffer();
      }
      writeBuffer.put(value);
    }

    @Override
    public void writeByte(int value) throws KryoException
    {
      if (!writeBuffer.hasRemaining()) {
        advanceWriteBuffer();
      }
      writeBuffer.put((byte)value);
    }

    @Override
    public void writeBytes(byte[] bytes) throws KryoException
    {
      write(bytes);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int count) throws KryoException
    {
      write(bytes, offset, count);
    }

    @Override
    public void writeInt(int value) throws KryoException
    {
      switch (writeBuffer.remaining()) {
        case 0:
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >> 24));
          writeBuffer.put((byte)(value >> 16));
          writeBuffer.put((byte)(value >> 8));
          writeBuffer.put((byte)value);
          break;

        case 1:
          writeBuffer.put((byte)(value >> 24));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >> 16));
          writeBuffer.put((byte)(value >> 8));
          writeBuffer.put((byte)value);
          break;

        case 2:
          writeBuffer.put((byte)(value >> 24));
          writeBuffer.put((byte)(value >> 16));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >> 8));
          writeBuffer.put((byte)value);
          break;

        case 3:
          writeBuffer.put((byte)(value >> 24));
          writeBuffer.put((byte)(value >> 16));
          writeBuffer.put((byte)(value >> 8));
          advanceWriteBuffer();
          writeBuffer.put((byte)value);
          break;

        default:
          writeBuffer.put((byte)(value >> 24));
          writeBuffer.put((byte)(value >> 16));
          writeBuffer.put((byte)(value >> 8));
          writeBuffer.put((byte)value);
          break;
      }
    }

    @Override
    public int writeInt(int value, boolean optimizePositive) throws KryoException
    {
      if (!optimizePositive) {
        value = (value << 1) ^ (value >> 31);
      }

      int remaining = writeBuffer.remaining();
      if (value >>> 7 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)value);
            break;

          default:
            writeBuffer.put((byte)value);
            break;
        }
        return 1;
      }

      if (value >>> 14 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7));
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;

          case 1:
            writeBuffer.put((byte)(value >>> 7));
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;

          default:
            writeBuffer.put((byte)(value >>> 7));
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;
        }
        return 2;
      }

      if (value >>> 21 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
        }
        return 3;
      }

      if (value >>> 28 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
        }
        return 4;
      }

      switch (remaining) {
        case 0:
          advanceWriteBuffer();
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28));
          break;
        case 1:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28));
          break;
        case 2:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28));
          break;
        case 3:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28));
          break;
        case 4:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 28));
          break;
        default:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28));
          break;
      }
      return 5;
    }

    @Override
    public void writeString(String value) throws KryoException
    {
      if (value == null) {
        writeByte(0x80); // 0 means null, bit 8 means UTF8.
        return;
      }
      int charCount = value.length();
      if (charCount == 0) {
        writeByte(1 | 0x80); // 1 means empty string, bit 8 means UTF8.
        return;
      }
      // Detect ASCII.
      boolean ascii = false;
      if (charCount > 1 && charCount < 64) {
        ascii = true;
        for (int i = 0; i < charCount; i++) {
          int c = value.charAt(i);
          if (c > 127) {
            ascii = false;
            break;
          }
        }
      }

      int charIndex = 0;
      if (ascii) {
        do {
          for (int i = writeBuffer.remaining(); i-- > 0 && charIndex < charCount;) {
            writeBuffer.put((byte)(value.charAt(charIndex++)));
          }
          if (charIndex < charCount) {
            advanceWriteBuffer();
          } else {
            break;
          }
        } while (true);

        int pos = writeBuffer.position() - 1;
        writeBuffer.put(pos, (byte)(writeBuffer.get(pos) | 0x80));
      } else {
        writeUtf8Length(charCount + 1);
        do {
          int c;
          for (int i = writeBuffer.remaining(); i-- > 0 && charIndex < charCount; charIndex++) {
            c = value.charAt(charIndex);
            if (c > 127) {
              writeString_slow(value, charCount, charIndex);
              return;
            }
            writeBuffer.put((byte)c);
          }

          if (charIndex < charCount) {
            advanceWriteBuffer();
          } else {
            break;
          }
        } while (true);
      }
    }

    @Override
    public void writeString(CharSequence value) throws KryoException
    {
      if (value == null) {
        writeByte(0x80); // 0 means null, bit 8 means UTF8.
        return;
      }
      int charCount = value.length();
      if (charCount == 0) {
        writeByte(1 | 0x80); // 1 means empty string, bit 8 means UTF8.
        return;
      }
      writeUtf8Length(charCount + 1);
      int charIndex = 0;
      do {
        int c;
        for (int i = writeBuffer.remaining(); i-- > 0; charIndex++) {
          c = value.charAt(charIndex);
          if (c > 127) {
            writeString_slow(value, charCount, charIndex);
            return;
          }
          writeBuffer.put((byte)c);
        }

        if (charIndex < charCount) {
          advanceWriteBuffer();
        } else {
          break;
        }
      } while (true);
    }

    @Override
    public void writeAscii(String value) throws KryoException
    {
      if (value == null) {
        writeByte(0x80); // 0 means null, bit 8 means UTF8.
        return;
      }
      int charCount = value.length();
      if (charCount == 0) {
        writeByte(1 | 0x80); // 1 means empty string, bit 8 means UTF8.
        return;
      }
      int charIndex = 0;
      do {
        for (int i = writeBuffer.remaining(); i-- > 0 && charIndex < charCount;) {
          writeBuffer.put((byte)(value.charAt(charIndex++)));
        }
        if (charIndex < charCount) {
          advanceWriteBuffer();
        } else {
          break;
        }
      } while (true);

      int pos = writeBuffer.position() - 1;
      writeBuffer.put(pos, (byte)(writeBuffer.get(pos) | 0x80));
    }

    @Override
    public void writeShort(int value) throws KryoException
    {
      if (writeBuffer.hasRemaining()) {
        writeBuffer.put((byte)(value >>> 8));
        if (writeBuffer.hasRemaining()) {
          writeBuffer.put((byte)value);
        } else {
          advanceWriteBuffer();
          writeBuffer.put((byte)value);
        }
      } else {
        advanceWriteBuffer();
        writeBuffer.put((byte)(value >>> 8));
        writeBuffer.put((byte)value);
      }
    }

    @Override
    public void writeLong(long value) throws KryoException
    {
      switch (writeBuffer.remaining()) {
        case 0:
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 1:
          writeBuffer.put((byte)(value >>> 56));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 2:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 3:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 4:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 5:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 6:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
        case 7:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          advanceWriteBuffer();
          writeBuffer.put((byte)value);
          break;
        default:
          writeBuffer.put((byte)(value >>> 56));
          writeBuffer.put((byte)(value >>> 48));
          writeBuffer.put((byte)(value >>> 40));
          writeBuffer.put((byte)(value >>> 32));
          writeBuffer.put((byte)(value >>> 24));
          writeBuffer.put((byte)(value >>> 16));
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
      }
    }

    @Override
    public int writeLong(long value, boolean optimizePositive) throws KryoException
    {
      if (!optimizePositive) {
        value = (value << 1) ^ (value >> 63);
      }

      int remaining = writeBuffer.remaining();
      if (value >>> 7 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)value);
            break;

          default:
            writeBuffer.put((byte)value);
            break;
        }
        return 1;
      }

      if (value >>> 14 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7));
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;

          case 1:
            writeBuffer.put((byte)(value >>> 7));
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;

          default:
            writeBuffer.put((byte)(value >>> 7));
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            break;
        }
        return 2;
      }

      if (value >>> 21 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14));
            break;
        }
        return 3;
      }

      if (value >>> 28 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21));
            break;
        }
        return 4;
      }

      if (value >>> 35 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28));
            break;
          case 4:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 28));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28));
            break;
        }
        return 5;
      }

      if (value >>> 42 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
          case 4:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
          case 5:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 35));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35));
            break;
        }
        return 6;
      }

      if (value >>> 49 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 4:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 5:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
          case 6:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 42));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42));
            break;
        }
        return 7;
      }

      if (value >>> 56 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 1:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 2:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 3:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 4:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 5:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 6:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
          case 7:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 49));
            break;
          default:
            writeBuffer.put((byte)((value & 0x7F) | 0x80));
            writeBuffer.put((byte)(value >>> 7 | 0x80));
            writeBuffer.put((byte)(value >>> 14 | 0x80));
            writeBuffer.put((byte)(value >>> 21 | 0x80));
            writeBuffer.put((byte)(value >>> 28 | 0x80));
            writeBuffer.put((byte)(value >>> 35 | 0x80));
            writeBuffer.put((byte)(value >>> 42 | 0x80));
            writeBuffer.put((byte)(value >>> 49));
            break;
        }
        return 8;
      }

      switch (remaining) {
        case 0:
          advanceWriteBuffer();
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 1:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 2:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 3:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 4:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 5:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 6:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 7:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
        case 8:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 56));
          break;
        default:
          writeBuffer.put((byte)((value & 0x7F) | 0x80));
          writeBuffer.put((byte)(value >>> 7 | 0x80));
          writeBuffer.put((byte)(value >>> 14 | 0x80));
          writeBuffer.put((byte)(value >>> 21 | 0x80));
          writeBuffer.put((byte)(value >>> 28 | 0x80));
          writeBuffer.put((byte)(value >>> 35 | 0x80));
          writeBuffer.put((byte)(value >>> 42 | 0x80));
          writeBuffer.put((byte)(value >>> 49 | 0x80));
          writeBuffer.put((byte)(value >>> 56));
          break;
      }
      return 9;
    }

    @Override
    public void writeBoolean(boolean value) throws KryoException
    {
      if (writeBuffer.hasRemaining()) {
        writeBuffer.put((byte)(value ? 1 : 0));
      } else {
        advanceWriteBuffer();
        writeBuffer.put((byte)(value ? 1 : 0));
      }
    }

    @Override
    public void writeChar(char value) throws KryoException
    {
      switch (writeBuffer.remaining()) {
        case 0:
          advanceWriteBuffer();
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;

        case 1:
          writeBuffer.put((byte)(value >>> 8));
          advanceWriteBuffer();
          writeBuffer.put((byte)value);
          break;

        default:
          writeBuffer.put((byte)(value >>> 8));
          writeBuffer.put((byte)value);
          break;
      }
    }

    private void writeUtf8Length(int value)
    {
      int remaining = writeBuffer.remaining();
      if (value >>> 6 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value | 0x80));
            break;

          default:
            writeBuffer.put((byte)(value | 0x80));
            break;
        }
      } else if (value >>> 13 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)(value >>> 6));
            break;

          case 1:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 6));
            break;

          default:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)(value >>> 6));
            break;
        }
      } else if (value >>> 20 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 13));
            break;
          case 1:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 13));
            break;
          case 2:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 13));
            break;
          default:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 13));
            break;
        }
      } else if (value >>> 27 == 0) {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 20));
            break;
          case 1:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 20));
            break;
          case 2:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 20));
            break;
          case 3:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 20));
            break;
          default:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 20));
            break;
        }
      } else {
        switch (remaining) {
          case 0:
            advanceWriteBuffer();
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 27));
            break;
          case 1:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 27));
            break;
          case 2:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 27));
            break;
          case 3:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 27));
            break;
          case 4:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            advanceWriteBuffer();
            writeBuffer.put((byte)(value >>> 27));
            break;
          default:
            writeBuffer.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            writeBuffer.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            writeBuffer.put((byte)(value >>> 27));
            break;
        }
      }

    }

    private void writeString_slow(CharSequence value, int charCount, int charIndex)
    {
      int remaining = writeBuffer.remaining();
      for (; charIndex < charCount; charIndex++) {
        int c = value.charAt(charIndex);
        if (c <= 0x007F) {
          switch (remaining) {
            case 0:
              advanceWriteBuffer();
              writeBuffer.put((byte)c);
              remaining = writeBuffer.remaining();
              break;

            default:
              writeBuffer.put((byte)c);
              remaining--;
              break;
          }
        } else if (c > 0x07FF) {
          switch (remaining) {
            case 0:
              advanceWriteBuffer();
              writeBuffer.put((byte)(0xE0 | c >> 12 & 0x0F));
              writeBuffer.put((byte)(0x80 | c >> 6 & 0x3F));
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining = writeBuffer.remaining();
              break;

            case 1:
              writeBuffer.put((byte)(0xE0 | c >> 12 & 0x0F));
              advanceWriteBuffer();
              writeBuffer.put((byte)(0x80 | c >> 6 & 0x3F));
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining = writeBuffer.remaining();
              break;

            case 2:
              writeBuffer.put((byte)(0xE0 | c >> 12 & 0x0F));
              writeBuffer.put((byte)(0x80 | c >> 6 & 0x3F));
              advanceWriteBuffer();
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining = writeBuffer.remaining();
              break;

            default:
              writeBuffer.put((byte)(0xE0 | c >> 12 & 0x0F));
              writeBuffer.put((byte)(0x80 | c >> 6 & 0x3F));
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining -= 3;
              break;
          }
        } else {
          switch (remaining) {
            case 0:
              advanceWriteBuffer();
              writeBuffer.put((byte)(0xC0 | c >> 6 & 0x1F));
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining = writeBuffer.remaining();
              break;
            case 1:
              writeBuffer.put((byte)(0xC0 | c >> 6 & 0x1F));
              advanceWriteBuffer();
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining = writeBuffer.remaining();
              break;

            default:
              writeBuffer.put((byte)(0xC0 | c >> 6 & 0x1F));
              writeBuffer.put((byte)(0x80 | c & 0x3F));
              remaining -= 2;
              break;
          }
        }
      }
    }

  };
  private static final Logger logger = LoggerFactory.getLogger(FastPublisher.class);

  @Override
  public void connected()
  {
    write = false;
  }

  @Override
  public void disconnected()
  {
    write = true;
  }

}
