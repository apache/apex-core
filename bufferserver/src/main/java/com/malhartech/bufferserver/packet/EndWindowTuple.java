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
public class EndWindowTuple extends WindowIdTuple
{
  public EndWindowTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    byte[] array = new byte[6];

    array[0] = MessageType.END_WINDOW_VALUE;
    Codec.writeRawVarint32(windowId, array, 1);

    return array;
  }

}
