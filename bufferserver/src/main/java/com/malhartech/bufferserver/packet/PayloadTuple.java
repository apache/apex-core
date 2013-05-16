/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.common.Fragment;

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
    int p = buffer[offset + 1];
    p |= buffer[offset + 2] << 8;
    p |= buffer[offset + 3] << 16;
    p |= buffer[offset + 4] << 24;
    return p;
  }

  @Override
  public int getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Fragment getData()
  {
    return new Fragment(buffer, offset + 5, length + offset - 5);
  }

  @Override
  public String toString()
  {
    return "PayloadTuple{" + getPartition() + ", " + getData() + '}';
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
    byte[] array = new byte[size + 5];
    array[0] = MessageType.PAYLOAD_VALUE;
    array[1] = (byte)partition;
    array[2] = (byte)(partition >> 8);
    array[3] = (byte)(partition >> 16);
    array[4] = (byte)(partition >> 24);
    return array;
  }

  public static byte[] getSerializedTuple(int partition, Fragment f)
  {
    byte[] array = new byte[5 + f.length];
    array[0] = MessageType.PAYLOAD_VALUE;
    array[1] = (byte)partition;
    array[2] = (byte)(partition >> 8);
    array[3] = (byte)(partition >> 16);
    array[4] = (byte)(partition >> 24);
    System.arraycopy(f.buffer, f.offset, array, 5, f.length);
    return array;
  }

}
