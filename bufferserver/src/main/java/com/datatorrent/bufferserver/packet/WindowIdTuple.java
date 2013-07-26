/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.Slice;

/**
 * <p>WindowIdTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
