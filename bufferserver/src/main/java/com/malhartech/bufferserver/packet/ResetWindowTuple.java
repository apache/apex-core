/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import com.malhartech.bufferserver.util.Codec;

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
  public int getDataOffset()
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

  public static byte[] getSerializedTuple(long windowId)
  {
    byte[] buffer = new byte[12];
    int offset = 0;

    buffer[offset++] = MessageType.RESET_WINDOW_VALUE;

    int baseSeconds = (int)(windowId >> 32);
    offset = Codec.writeRawVarint32(baseSeconds, buffer, offset);

    Codec.writeRawVarint32((int)windowId, buffer, offset);

    return buffer;
  }

}
