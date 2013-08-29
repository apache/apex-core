/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>EndWindowTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class EndWindowTuple extends WindowIdTuple
{
  private static final byte[][] serializedTuples = new byte[16000][];

  static {
    for (int i = serializedTuples.length; i-- > 0;) {
      serializedTuples[i] = WindowIdTuple.getSerializedTuple(i);
      serializedTuples[i][0] = MessageType.END_WINDOW_VALUE;
    }
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    return serializedTuples[windowId % serializedTuples.length];
  }

  public EndWindowTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

}
