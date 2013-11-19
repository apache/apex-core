/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;


import com.datatorrent.common.util.Slice;
import com.datatorrent.common.util.VarInt;

/**
 * <p>ResetWindowTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
    size = VarInt.write(baseSeconds, buffer, size);
    VarInt.write(windowWidth, buffer, size);

    return buffer;
  }

}
