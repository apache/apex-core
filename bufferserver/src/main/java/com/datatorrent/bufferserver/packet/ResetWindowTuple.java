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
public class ResetWindowTuple extends Tuple
{
  public ResetWindowTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
  }

  @Override
  public MessageType getType()
  {
    return MessageType.RESET_WINDOW;
  }

  @Override
  public int getWindowId()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    return readVarInt(offset + 1, offset + length);
  }

  @Override
  public int getWindowWidth()
  {
    int intervalOffset = offset + 1;
    while (buffer[intervalOffset++] < 0) {
    }
    return readVarInt(intervalOffset, offset + length);
  }

  public static byte[] getSerializedTuple(int baseSeconds, int windowWidth)
  {
    int size = 1; /* for type */

    /* for baseSeconds */
    int bits = 32 - Integer.numberOfLeadingZeros(baseSeconds);
    size += bits / 7 + 1;

    /* for windowWidth */
    bits = 32 - Integer.numberOfLeadingZeros(windowWidth);
    size += bits / 7 + 1;

    byte[] buffer = new byte[size];
    size = 0;

    buffer[size++] = MessageType.RESET_WINDOW_VALUE;
    size = Codec.writeRawVarint32(baseSeconds, buffer, size);
    Codec.writeRawVarint32(windowWidth, buffer, size);

    return buffer;
  }

}
