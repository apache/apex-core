/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.bufferserver.util.Codec;
import com.malhartech.common.util.Slice;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowIdTuple extends Tuple
{
  public WindowIdTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public int getWindowId()
  {
    return readVarInt(offset + 1, offset + length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.valueOf(buffer[offset]);
  }

  @Override
  public int getPartition()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Slice getData()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

  @Override
  public String toString()
  {
    return "WindowIdTuple{" + getType() + ", " + Integer.toHexString(getWindowId()) + '}';
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    int offset = 1; /* for type */

    int bits = 32 - Integer.numberOfLeadingZeros(windowId);
    offset += bits / 7 + 1;

    byte[] array = new byte[offset];
    Codec.writeRawVarint32(windowId, array, 1);

    return array;
  }

}
