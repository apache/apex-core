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
public class BeginWindowTuple extends WindowIdTuple
{
  public BeginWindowTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    byte[] array = new byte[6];
    int offset = 0;

    array[offset++] = MessageType.BEGIN_WINDOW_VALUE;
    Codec.writeRawVarint32(windowId, array, offset);

    return array;
  }
}
