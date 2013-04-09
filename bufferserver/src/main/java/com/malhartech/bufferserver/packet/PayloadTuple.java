/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.bufferserver.util.Codec;
import com.malhartech.netlet.Client.Fragment;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PayloadTuple extends Tuple
{
  public PayloadTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.PAYLOAD;
  }

  @Override
  public int getPartition()
  {
    return readVarInt(offset + 1, offset + length);
  }

  @Override
  public int getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Fragment getData()
  {
    int dataOffset = this.offset + 1;
    while (buffer[dataOffset++] < 0) {
    }

    return new Fragment(buffer, dataOffset, length + offset - dataOffset);
  }

  @Override
  public String toString()
  {
    return "PayloadTuple{" + getPartition() + ", " + getData() +  '}';
  }

  @Override
  public int getBaseSeconds()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getWindowWidth()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public static byte[] getSerializedTuple(int partition, int size)
  {
    int bits = 32 - Integer.numberOfLeadingZeros(partition);
    do {
      size++;
    }
    while ((bits -= 7) > 0);

    byte[] array = new byte[size + 1];
    array[0] = MessageType.PAYLOAD_VALUE;
    Codec.writeRawVarint32(partition, array, 1);
    return array;
  }

  public static byte[] getSerializedTuple(int partition, Fragment f)
  {
    int size = 0;
    int bits = 32 - Integer.numberOfLeadingZeros(partition);
    do {
      size++;
    }
    while ((bits -= 7) > 0);
    size++;

    byte[] array = new byte[size + f.length];
    array[0] = MessageType.PAYLOAD_VALUE;
    Codec.writeRawVarint32(partition, array, 1);
    System.arraycopy(f.buffer, f.offset, array, size, f.length);
    return array;
  }

}
