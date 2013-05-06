/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.netlet.DefaultEventLoop;
import com.malhartech.netlet.EventLoop;
import com.malhartech.netlet.Listener;
import com.malhartech.netlet.Listener.ClientListener;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class FastPublisher implements ClientListener, Stream
{
  public static final int EIGHT_KILOBYTES = 8 * 1024;
  private StreamCodec<Object> codec;
  private SelectionKey key;
  private EventLoop eventloop;
  private int count;
  private long spinMillis;
  private ByteBuffer[] buffer;
  private ByteBuffer[] overflowBuffer;
  private volatile int writer;
  private volatile int reader;
  private volatile int readPosition;
  private AtomicBoolean inProgress = new AtomicBoolean(false);

  public FastPublisher(int countOf8kBuffers)
  {
    buffer = new ByteBuffer[countOf8kBuffers];
    for (int i = countOf8kBuffers; i-- > 0;) {
      buffer[i] = ByteBuffer.allocateDirect(EIGHT_KILOBYTES);
    }
  }

  @Override
  public void read() throws IOException
  {
    SocketChannel channel = (SocketChannel)key.channel();
    int read;
    if ((read = channel.read(ByteBuffer.allocate(1))) > 0) {
      throw new RuntimeException("Publisher " + this + " is not supposed to receive any data");
    }
    else if (read == -1) {
      try {
        channel.close();
      }
      finally {
        unregistered(key);
        key.attach(Listener.NOOP_CLIENT_LISTENER);
      }
    }
    else {
      logger.debug("{} read 0 bytes", this);
    }
  }

  @Override
  public void write() throws IOException
  {
    SocketChannel sc = (SocketChannel)key.channel();
    while (reader != writer) {
      ByteBuffer bb = buffer[reader];
      sc.write(bb);
      if (bb.hasRemaining()) {
        return;
      }
      else {
        bb.clear();
        reader++;
      }
    }

    /* synchronize the access */
    if (inProgress.compareAndSet(false, true)) {
      try {
        ByteBuffer bb = buffer[reader];
        if (writer == reader) {
          /* writer has still not finished writing to this block */
          int writePosition = bb.position();
          bb.flip();
          bb.position(readPosition);
          sc.write(bb);
          readPosition = bb.position();
          bb.clear();
          bb.position(writePosition);
        }
        else {
          /* writer moved on in the meantime */
          sc.write(bb);
          if (!bb.hasRemaining()) {
            bb.clear();
            reader++;
          }
        }
      }
      finally {
        inProgress.set(false);
      }
    }
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
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
  public boolean isMultiSinkCapable()
  {
    return false;
  }

  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    throw new UnsupportedOperationException("setSink(id, sink) not supported on this stream.");
  }

  @Override
  public void setup(StreamContext context)
  {
    spinMillis = 5; // somehow get it context.attrValue(PortContext.SPIN_MILLIS, 5);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.attr(StreamContext.EVENT_LOOP).get();
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), context.getStartingWindowId(), context.getBufferServerAddress()});
    codec = context.attr(StreamContext.CODEC).get();
  }

  @Override
  public void deactivate()
  {
    eventloop.disconnect(this);
  }

  @Override
  public void put(Object tuple)
  {
    count++;
    while (inProgress.compareAndSet(false, true)) {
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    if (reset) {
      try {
        return count;
      }
      finally {
        count = 0;
      }
    }

    return count;
  }

  private final Output output = new Output()
  {
    ByteBuffer bb;

    @SuppressWarnings("SleepWhileInLoop")
    private void getNextBuffer()
    {
      int nextWriter = writer + 1;
      if (nextWriter == buffer.length) {
        nextWriter = 0;
      }

      if (nextWriter == reader) {
        inProgress.set(false);
        try {
          do {
            sleep(spinMillis);
          }
          while (nextWriter == reader);
        }
        catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
        inProgress.set(true);
      }

      writer = nextWriter;
      bb = buffer[writer];
    }

    @Override
    public void write(int value) throws KryoException
    {
      if (!bb.hasRemaining()) {
        getNextBuffer();
      }
      bb.put((byte)value);
    }

    @Override
    public void write(byte[] bytes) throws KryoException
    {
      int remaining = bb.remaining();
      if (bytes.length > remaining) {
        bb.put(bytes, 0, remaining);
        getNextBuffer();
        write(bytes, remaining, bytes.length - remaining);
      }
      else {
        bb.put(bytes);
      }
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws KryoException
    {
      int remaining = bb.remaining();
      while (length > remaining) {
        bb.put(bytes, offset, remaining);
        offset += remaining;
        length -= remaining;
        getNextBuffer();
        remaining = bb.remaining();
      }

      bb.put(bytes, offset, length);
    }

    @Override
    public void writeByte(byte value) throws KryoException
    {
      if (!bb.hasRemaining()) {
        getNextBuffer();
      }
      bb.put(value);
    }

    @Override
    public void writeByte(int value) throws KryoException
    {
      if (!bb.hasRemaining()) {
        getNextBuffer();
      }
      bb.put((byte)value);
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
      int i = 0;
      switch (bb.remaining()) {
        case 0:
          getNextBuffer();
          bb.putInt(value);
          break;

        case 1:
          bb.put((byte)(value >> 24));
          getNextBuffer();
          bb.put((byte)(value >> 16));
          bb.put((byte)(value >> 8));
          bb.put((byte)value);
          break;

        case 2:
          bb.put((byte)(value >> 24));
          bb.put((byte)(value >> 16));
          getNextBuffer();
          bb.put((byte)(value >> 8));
          bb.put((byte)value);
          break;

        case 3:
          bb.put((byte)(value >> 24));
          bb.put((byte)(value >> 16));
          bb.put((byte)(value >> 8));
          getNextBuffer();
          bb.put((byte)value);
          break;

        default:
          bb.put((byte)(value >> 24));
          bb.put((byte)(value >> 16));
          bb.put((byte)(value >> 8));
          bb.put((byte)value);
          break;
      }
    }

    @Override
    public int writeInt(int value, boolean optimizePositive) throws KryoException
    {
      if (!optimizePositive) {
        value = (value << 1) ^ (value >> 31);
      }

      int remaining = bb.remaining();
      if (value >>> 7 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)value);
            break;

          default:
            bb.put((byte)value);
            break;
        }
        return 1;
      }

      if (value >>> 14 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value >>> 7));
            bb.put((byte)((value & 0x7F) | 0x80));
            break;

          case 1:
            bb.put((byte)(value >>> 7));
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            break;

          default:
            bb.put((byte)(value >>> 7));
            bb.put((byte)((value & 0x7F) | 0x80));
            break;
        }
        return 2;
      }

      if (value >>> 21 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
        }
        return 3;
      }

      if (value >>> 28 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 3:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 21));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
        }
        return 4;
      }

      switch (remaining) {
        case 0:
          getNextBuffer();
          bb.put((byte)((value & 0x7F) | 0x80));
          bb.put((byte)(value >>> 7 | 0x80));
          bb.put((byte)(value >>> 14 | 0x80));
          bb.put((byte)(value >>> 21 | 0x80));
          bb.put((byte)(value >>> 28));
          break;
        case 1:
          bb.put((byte)((value & 0x7F) | 0x80));
          getNextBuffer();
          bb.put((byte)(value >>> 7 | 0x80));
          bb.put((byte)(value >>> 14 | 0x80));
          bb.put((byte)(value >>> 21 | 0x80));
          bb.put((byte)(value >>> 28));
          break;
        case 2:
          bb.put((byte)((value & 0x7F) | 0x80));
          bb.put((byte)(value >>> 7 | 0x80));
          getNextBuffer();
          bb.put((byte)(value >>> 14 | 0x80));
          bb.put((byte)(value >>> 21 | 0x80));
          bb.put((byte)(value >>> 28));
          break;
        case 3:
          bb.put((byte)((value & 0x7F) | 0x80));
          bb.put((byte)(value >>> 7 | 0x80));
          bb.put((byte)(value >>> 14 | 0x80));
          getNextBuffer();
          bb.put((byte)(value >>> 21 | 0x80));
          bb.put((byte)(value >>> 28));
          break;
        case 4:
          bb.put((byte)((value & 0x7F) | 0x80));
          bb.put((byte)(value >>> 7 | 0x80));
          bb.put((byte)(value >>> 14 | 0x80));
          bb.put((byte)(value >>> 21 | 0x80));
          getNextBuffer();
          bb.put((byte)(value >>> 28));
          break;
        default:
          bb.put((byte)((value & 0x7F) | 0x80));
          bb.put((byte)(value >>> 7 | 0x80));
          bb.put((byte)(value >>> 14 | 0x80));
          bb.put((byte)(value >>> 21 | 0x80));
          bb.put((byte)(value >>> 28));
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

      if (ascii) {
        int charIndex = 0;
        do {
          for (int i = bb.remaining(); i-- > 0 && charIndex < charCount;) {
            bb.put((byte)(value.charAt(charIndex++)));
          }
          if (charIndex < charCount) {
            getNextBuffer();
            continue;
          }
          break;
        }
        while (true);

        int pos = bb.position() - 1;
        bb.put(pos, (byte)(bb.get(pos) | 0x80));
      }
      else {
        writeUtf8Length(charCount + 1);
        int charIndex = 0;
        do {
          int c;
          for (int i = bb.remaining(); i-- > 0; charIndex++) {
            c = value.charAt(charIndex);
            if (c > 127) {
              writeString_slow(value, charCount, charIndex);
              return;
            }
            bb.put((byte)c);
          }

          if (charIndex < charCount) {
            getNextBuffer();
            continue;
          }
        }
        while (true);
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
        for (int i = bb.remaining(); i-- > 0; charIndex++) {
          c = value.charAt(charIndex);
          if (c > 127) {
            writeString_slow(value, charCount, charIndex);
            return;
          }
          bb.put((byte)c);
        }

        if (charIndex < charCount) {
          getNextBuffer();
          continue;
        }
      }
      while (true);
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
        for (int i = bb.remaining(); i-- > 0 && charIndex < charCount;) {
          bb.put((byte)(value.charAt(charIndex++)));
        }
        if (charIndex < charCount) {
          getNextBuffer();
          continue;
        }
        break;
      }
      while (true);

      int pos = bb.position() - 1;
      bb.put(pos, (byte)(bb.get(pos) | 0x80));
    }

    @Override
    public void writeShort(int value) throws KryoException
    {
      if (bb.hasRemaining()) {
        bb.put((byte)(value >>> 8));
        if (bb.hasRemaining()) {
          bb.put((byte)value);
        }
        else {
          getNextBuffer();
          bb.put((byte)value);
        }
      }
      else {
        getNextBuffer();
        bb.put((byte)(value >>> 8));
        bb.put((byte)value);
      }
    }

    @Override
    public void writeLong(long value) throws KryoException
    {
      switch (bb.remaining()) {
        case 0:
          getNextBuffer();
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 1:
          bb.put((byte)(value >>> 56));
          getNextBuffer();
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 2:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          getNextBuffer();
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 3:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          getNextBuffer();
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 4:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          getNextBuffer();
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 5:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          getNextBuffer();
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 6:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          getNextBuffer();
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
        case 7:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          getNextBuffer();
          bb.put((byte)value);
          break;
        default:
          bb.put((byte)(value >>> 56));
          bb.put((byte)(value >>> 48));
          bb.put((byte)(value >>> 40));
          bb.put((byte)(value >>> 32));
          bb.put((byte)(value >>> 24));
          bb.put((byte)(value >>> 16));
          bb.put((byte)(value >>> 8));
          bb.put((byte)value);
          break;
      }
    }

    @Override
    public int writeLong(long value, boolean optimizePositive) throws KryoException
    {
      if (!optimizePositive) {
        value = (value << 1) ^ (value >> 63);
      }

      int remaining = bb.remaining();
      if (value >>> 7 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)value);
            break;

          default:
            bb.put((byte)value);
            break;
        }
        return 1;
      }

      if (value >>> 14 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value >>> 7));
            bb.put((byte)((value & 0x7F) | 0x80));
            break;

          case 1:
            bb.put((byte)(value >>> 7));
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            break;

          default:
            bb.put((byte)(value >>> 7));
            bb.put((byte)((value & 0x7F) | 0x80));
            break;
        }
        return 2;
      }

      if (value >>> 21 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14));
            break;
        }
        return 3;
      }

      if (value >>> 28 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
          case 3:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 21));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21));
            break;
        }
        return 4;
      }

      if (value >>> 35 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28));
            break;
          case 3:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28));
            break;
          case 4:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 28));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28));
            break;
        }
        return 5;
      }

      if (value >>> 42 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
          case 3:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
          case 4:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
          case 5:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 35));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35));
            break;
        }
        return 6;
      }

      if (value >>> 49 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 1:
            bb.put((byte)((value & 0x7F) | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 2:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 3:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 4:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 5:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
          case 6:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            getNextBuffer();
            bb.put((byte)(value >>> 42));
            break;
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42));
            break;
        }
        return 7;
      }

      if (value >>> 56 == 0) {
        switch (remaining) {
          default:
            bb.put((byte)((value & 0x7F) | 0x80));
            bb.put((byte)(value >>> 7 | 0x80));
            bb.put((byte)(value >>> 14 | 0x80));
            bb.put((byte)(value >>> 21 | 0x80));
            bb.put((byte)(value >>> 28 | 0x80));
            bb.put((byte)(value >>> 35 | 0x80));
            bb.put((byte)(value >>> 42 | 0x80));
            bb.put((byte)(value >>> 49));
            break;
        }
        return 8;
      }

      bb.put((byte)((value & 0x7F) | 0x80));
      bb.put((byte)(value >>> 7 | 0x80));
      bb.put((byte)(value >>> 14 | 0x80));
      bb.put((byte)(value >>> 21 | 0x80));
      bb.put((byte)(value >>> 28 | 0x80));
      bb.put((byte)(value >>> 35 | 0x80));
      bb.put((byte)(value >>> 42 | 0x80));
      bb.put((byte)(value >>> 49 | 0x80));
      bb.put((byte)(value >>> 56));
      return 9;
    }

    @Override
    public void writeBoolean(boolean value) throws KryoException
    {
      super.writeBoolean(value);
    }

    @Override
    public void writeChar(char value) throws KryoException
    {
      super.writeChar(value);
    }

    @Override
    public void writeDouble(double value) throws KryoException
    {
      super.writeDouble(value);
    }

    @Override
    public int writeDouble(double value, double precision, boolean optimizePositive) throws KryoException
    {
      return super.writeDouble(value, precision, optimizePositive);
    }

    private void writeUtf8Length(int value)
    {
      int remaining = bb.remaining();
      if (value >>> 6 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value | 0x80));
            break;

          default:
            bb.put((byte)(value | 0x80));
            break;
        }
      }
      else if (value >>> 13 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)(value >>> 6));
            break;

          case 1:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            getNextBuffer();
            bb.put((byte)(value >>> 6));
            break;

          default:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)(value >>> 6));
            break;
        }
      }
      else if (value >>> 20 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 13));
            break;
          case 1:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            getNextBuffer();
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 13));
            break;
          case 2:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)(value >>> 13));
            break;
          default:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 13));
            break;
        }
      }
      else if (value >>> 27 == 0) {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 20));
            break;
          case 1:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            getNextBuffer();
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 20));
            break;
          case 2:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 20));
            break;
          case 3:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)(value >>> 20));
            break;
          default:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 20));
            break;
        }
      }
      else {
        switch (remaining) {
          case 0:
            getNextBuffer();
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 27));
            break;
          case 1:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            getNextBuffer();
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 27));
            break;
          case 2:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 27));
            break;
          case 3:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 27));
            break;
          case 4:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            getNextBuffer();
            bb.put((byte)(value >>> 27));
            break;
          default:
            bb.put((byte)(value | 0x40 | 0x80)); // Set bit 7 and 8.
            bb.put((byte)((value >>> 6) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 13) | 0x80)); // Set bit 8.
            bb.put((byte)((value >>> 20) | 0x80)); // Set bit 8.
            bb.put((byte)(value >>> 27));
            break;
        }
      }

    }

    private void writeString_slow(CharSequence value, int charCount, int charIndex)
    {
      int remaining = bb.remaining();
      for (; charIndex < charCount; charIndex++) {
        int c = value.charAt(charIndex);
        if (c <= 0x007F) {
          switch (remaining) {
            case 0:
              getNextBuffer();
              bb.put((byte)c);
              remaining = bb.remaining();
              break;

            default:
              bb.put((byte)c);
              remaining--;
              break;
          }
        }
        else if (c > 0x07FF) {
          switch (remaining) {
            case 0:
              getNextBuffer();
              bb.put((byte)(0xE0 | c >> 12 & 0x0F));
              bb.put((byte)(0x80 | c >> 6 & 0x3F));
              bb.put((byte)(0x80 | c & 0x3F));
              remaining = bb.remaining();
              break;

            case 1:
              bb.put((byte)(0xE0 | c >> 12 & 0x0F));
              getNextBuffer();
              bb.put((byte)(0x80 | c >> 6 & 0x3F));
              bb.put((byte)(0x80 | c & 0x3F));
              remaining = bb.remaining();
              break;

            case 2:
              bb.put((byte)(0xE0 | c >> 12 & 0x0F));
              bb.put((byte)(0x80 | c >> 6 & 0x3F));
              getNextBuffer();
              bb.put((byte)(0x80 | c & 0x3F));
              remaining = bb.remaining();
              break;

            default:
              bb.put((byte)(0xE0 | c >> 12 & 0x0F));
              bb.put((byte)(0x80 | c >> 6 & 0x3F));
              bb.put((byte)(0x80 | c & 0x3F));
              remaining -= 3;
              break;
          }
        }
        else {
          switch (remaining) {
            case 0:
              getNextBuffer();
              bb.put((byte)(0xC0 | c >> 6 & 0x1F));
              bb.put((byte)(0x80 | c & 0x3F));
              remaining = bb.remaining();
              break;
            case 1:
              bb.put((byte)(0xC0 | c >> 6 & 0x1F));
              getNextBuffer();
              bb.put((byte)(0x80 | c & 0x3F));
              remaining = bb.remaining();
              break;

            default:
              bb.put((byte)(0xC0 | c >> 6 & 0x1F));
              bb.put((byte)(0x80 | c & 0x3F));
              remaining -= 2;
              break;
          }
        }
      }
    }

  };
  private static final Logger logger = LoggerFactory.getLogger(FastPublisher.class);
}
