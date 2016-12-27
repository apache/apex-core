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
package org.apache.apex.engine.serde;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.netlet.util.Slice;

public class SerializationBuffer extends Output implements WindowCompleteListener, WindowListener
{
  /*
   * Singleton read buffer for serialization
   */
  public static final SerializationBuffer READ_BUFFER = new SerializationBuffer(new WindowedBlockStream());

  private WindowedBlockStream windowedBlockStream;

  @SuppressWarnings("unused")
  private SerializationBuffer()
  {
    this(new WindowedBlockStream());
  }

  public SerializationBuffer(WindowedBlockStream windowedBlockStream)
  {
    //windowedBlockStream in fact same as outputStream
    super(windowedBlockStream);
    this.windowedBlockStream = windowedBlockStream;
  }

  public long size()
  {
    return windowedBlockStream.size();
  }

  public long capacity()
  {
    return windowedBlockStream.capacity();
  }

  /**
   * This method should be called only after the whole object has been written
   * @return The slice which represents the object
   */
  public Slice toSlice()
  {
    return windowedBlockStream.toSlice();
  }

  /**
   * reset the environment to reuse the resource.
   */
  public void reset()
  {
    windowedBlockStream.reset();
  }


  @Override
  public void beginWindow(long windowId)
  {
    windowedBlockStream.beginWindow(windowId);
  }

  @Override
  public void endWindow()
  {
    windowedBlockStream.endWindow();
  }

  public void release()
  {
    windowedBlockStream.releaseAllFreeMemory();;
  }

  public WindowedBlockStream createWindowedBlockStream()
  {
    return new WindowedBlockStream();
  }

  public WindowedBlockStream createWindowedBlockStream(int capacity)
  {
    return new WindowedBlockStream(capacity);
  }

  public WindowedBlockStream getWindowedBlockStream()
  {
    return windowedBlockStream;
  }

  public void setWindowableByteStream(WindowedBlockStream windowableByteStream)
  {
    this.windowedBlockStream = windowableByteStream;
  }

  /**
   * reset for all windows with window id less than or equal to the input windowId
   * this interface doesn't call reset window for each windows. Several windows can be reset at the same time.
   * @param windowId
   */
  @Override
  public void completeWindow(long windowId)
  {
    windowedBlockStream.completeWindow(windowId);
  }

  public byte[] toByteArray()
  {
    return toSlice().toByteArray();
  }

  /**
   * following methods override the super class method to avoid use temporary buffer
   */

  /**
   * This method was called by super class to make sure the buffer is enough.
   */
  @Override
  protected boolean require(int required) throws KryoException
  {
    Slice slice = this.windowedBlockStream.reserve(required);
    buffer = slice.buffer;
    position = slice.offset;
    return true;
  }

  /** Writes a byte. */
  @Override
  public void write(int value) throws KryoException
  {
    windowedBlockStream.write(value);
  }

  /** Writes the bytes. Note the byte[] length is not written. */
  @Override
  public void write(byte[] bytes) throws KryoException
  {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null.");
    }
    windowedBlockStream.write(bytes);
  }

  /** Writes the bytes. Note the byte[] length is not written. */
  @Override
  public void write(byte[] bytes, int offset, int length) throws KryoException
  {
    windowedBlockStream.write(bytes, offset, length);
  }

  // byte

  @Override
  public void writeByte(byte value) throws KryoException
  {
    write(value);
  }

  @Override
  public void writeByte(int value) throws KryoException
  {
    write(value);
  }

  /** Writes the bytes. Note the byte[] length is not written. */
  @Override
  public void writeBytes(byte[] bytes) throws KryoException
  {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null.");
    }
    write(bytes, 0, bytes.length);
  }

  /** Writes the bytes. Note the byte[] length is not written. */
  @Override
  public void writeBytes(byte[] bytes, int offset, int count) throws KryoException
  {
    write(bytes, offset, count);
  }

  // int

  /** Writes a 4 byte int. Uses BIG_ENDIAN byte order. */
  private byte[] intBytes = new byte[4];

  @Override
  public void writeInt(int value) throws KryoException
  {
    Slice slice = this.reserve(4);
    int offset = slice.offset;
    byte[] buffer = slice.buffer;
    buffer[offset++] = (byte)(value >> 24);
    buffer[offset++] = (byte)(value >> 16);
    buffer[offset++] = (byte)(value >> 8);
    buffer[offset] = (byte)value;
  }

  /**
   * Writes a 1-5 byte int. It is guaranteed that a varible length encoding will
   * be used.
   *
   * @param optimizePositive
   *          If true, small positive numbers will be more efficient (1 byte)
   *          and small negative numbers will be inefficient (5 bytes).
   */
  @Override
  public int writeVarInt(int value, boolean optimizePositive) throws KryoException
  {
    if (!optimizePositive) {
      value = (value << 1) ^ (value >> 31);
    }
    if (value >>> 7 == 0) {
      write(value);
      return 1;
    }
    byte[] buffer;
    int offset;
    if (value >>> 14 == 0) {
      Slice slice = this.reserve(2);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)((value & 0x7F) | 0x80);
      buffer[slice.offset] = (byte)(value >>> 7);
      return 2;
    }
    if (value >>> 21 == 0) {
      Slice slice = this.reserve(3);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)((value & 0x7F) | 0x80);
      buffer[offset++] = (byte)(value >>> 7 | 0x80);
      buffer[offset] = (byte)(value >>> 14);
      return 3;
    }
    if (value >>> 28 == 0) {
      Slice slice = this.reserve(4);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)((value & 0x7F) | 0x80);
      buffer[offset++] = (byte)(value >>> 7 | 0x80);
      buffer[offset++] = (byte)(value >>> 14 | 0x80);
      buffer[offset] = (byte)(value >>> 21);
      return 4;
    }
    Slice slice = this.reserve(5);
    buffer = slice.buffer;
    offset = slice.offset;
    buffer[offset++] = (byte)((value & 0x7F) | 0x80);
    buffer[offset++] = (byte)(value >>> 7 | 0x80);
    buffer[offset++] = (byte)(value >>> 14 | 0x80);
    buffer[offset++] = (byte)(value >>> 21 | 0x80);
    buffer[offset] = (byte)(value >>> 28);
    return 5;
  }

  // string

  /**
   * Writes the length and string, or null. Short strings are checked and if
   * ASCII they are written more efficiently, else they are written as UTF8. If
   * a string is known to be ASCII, {@link #writeAscii(String)} may be used. The
   * string can be read using {@link Input#readString()} or
   * {@link Input#readStringBuilder()}.
   *
   * @param value
   *          May be null.
   */
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
      writeAscii(value, charCount);
    } else {
      writeUtf8Length(charCount + 1);

      //treat as ascii first
      Slice slice = reserve(charCount);
      int index = slice.offset;
      //assign to buffer increase performance a lot
      byte[] buffer = slice.buffer;
      int charIndex = 0;
      for (; charIndex < charCount; charIndex++) {
        int c = value.charAt(charIndex);
        if (c > 127) {
          break;
        }
        buffer[index++] = (byte)c;
      }
      if (charCount > charIndex) {
        writeString(value, charCount, charIndex);
      }
    }
  }

  /**
   * write ascii, the last char | 0x80 to mark the end
   *
   * @param value
   * @param charCount
   * @throws KryoException
   */
  private void writeAscii(String value, int charCount) throws KryoException
  {
    Slice slice = reserve(charCount);
    value.getBytes(0, charCount, slice.buffer, slice.offset);
    slice.buffer[slice.offset + charCount - 1] |= 0x80;
  }

  /**
   * Writes the length of a string, which is a variable length encoded int
   * except the first byte uses bit 8 to denote UTF8 and bit 7 to denote if
   * another byte is present.
   */
  private void writeUtf8Length(int value)
  {
    if (value >>> 6 == 0) {
      write(value | 0x80); // Set bit 8.
      return;
    }
    byte[] buffer;
    int offset;
    if (value >>> 13 == 0) {
      Slice slice = reserve(2);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)(value | 0x40 | 0x80); // Set bit 7 and 8.
      buffer[offset] = (byte)(value >>> 6);
      return;
    }
    if (value >>> 20 == 0) {
      Slice slice = reserve(3);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)(value | 0x40 | 0x80); // Set bit 7 and 8.
      buffer[offset++] = (byte)((value >>> 6) | 0x80); // Set bit 8.
      buffer[offset] = (byte)(value >>> 13);
      return;
    }
    if (value >>> 27 == 0) {
      Slice slice = reserve(4);
      buffer = slice.buffer;
      offset = slice.offset;
      buffer[offset++] = (byte)(value | 0x40 | 0x80); // Set bit 7 and 8.
      buffer[offset++] = (byte)((value >>> 6) | 0x80); // Set bit 8.
      buffer[offset++] = (byte)((value >>> 13) | 0x80); // Set bit 8.
      buffer[offset] = (byte)(value >>> 20);
      return;
    }

    Slice slice = reserve(5);
    buffer = slice.buffer;
    offset = slice.offset;
    buffer[offset++] = (byte)(value | 0x40 | 0x80); // Set bit 7 and 8.
    buffer[offset++] = (byte)((value >>> 6) | 0x80); // Set bit 8.
    buffer[offset++] = (byte)((value >>> 13) | 0x80); // Set bit 8.
    buffer[offset++] = (byte)((value >>> 20) | 0x80); // Set bit 8.
    buffer[offset] = (byte)(value >>> 27);
  }

  private void writeString(CharSequence value, int charCount, final int charIndex)
  {
    //count serialized size
    int requiredSize = 0;
    for (int i = charIndex; i < charCount; i++) {
      int c = value.charAt(i);
      if (c <= 0x007F) {
        requiredSize++;
      } else if (c > 0x07FF) {
        requiredSize += 3;
      } else {
        requiredSize += 2;
      }
    }

    Slice slice = reserve(requiredSize);
    int offset = slice.offset;
    //assign to buffer increase performance a lot
    byte[] buffer = slice.buffer;
    for (int i = charIndex; i < charCount; i++) {
      int c = value.charAt(i);
      if (c <= 0x007F) {
        buffer[offset++] = (byte)c;
      } else if (c > 0x07FF) {
        buffer[offset++] = (byte)(0xE0 | c >> 12 & 0x0F);
        buffer[offset++] = (byte)(0x80 | c >> 6 & 0x3F);
        buffer[offset++] = (byte)(0x80 | c & 0x3F);
      } else {
        buffer[offset++] = (byte)(0xC0 | c >> 6 & 0x1F);
        buffer[offset++] = (byte)(0x80 | c & 0x3F);
      }
    }
  }

  /**
   * reserve the memory for future use. the reserve operation can happened
   * before/after or in the middle serialization
   *
   * @param length
   * @return the Slice of the reserved memory. the length of the slice will be
   *         same as the required length
   */
  public Slice reserve(int length)
  {
    return windowedBlockStream.reserve(length);
  }
}

