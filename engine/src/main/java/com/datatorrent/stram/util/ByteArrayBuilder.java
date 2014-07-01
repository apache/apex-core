/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.util;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class ByteArrayBuilder
{
  private static final int DEFAULT_BUFFER_SIZE = 1024;

  private byte[] buffer;
  private int cursor;

  public ByteArrayBuilder(int size)
  {
    buffer = new byte[size];
  }

  public ByteArrayBuilder()
  {
    this(DEFAULT_BUFFER_SIZE);
  }

  public void append(byte b)
  {
    if (cursor + 1 > buffer.length) {
      enlargeBuffer(cursor + 1);
    }
    buffer[cursor++] = b;
  }

  public void append(byte[] data, int offset, int length)
  {
    if (cursor + length > buffer.length) {
      enlargeBuffer(cursor + length);
    }
    System.arraycopy(data, offset, buffer, cursor, length);
    cursor += length;
  }

  public void append(byte[] data)
  {
    append(data, 0, data.length);
  }

  public byte[] toByteArray()
  {
    byte[] result = new byte[cursor];
    System.arraycopy(buffer, 0, result, 0, cursor);
    return result;
  }

  @Override
  public String toString()
  {
    return new String(buffer, 0, cursor);
  }
  
  public int length()
  {
    return cursor;
  }

  private void enlargeBuffer(int minLength)
  {
    int n = buffer.length;
    while (n < minLength) {
      n *= 2;
    }
    byte[] newBuffer = new byte[n];
    System.arraycopy(buffer, 0, newBuffer, 0, cursor);
    buffer = newBuffer;
  }

  public void setLength(int i)
  {
    cursor = 0;
  }

}
