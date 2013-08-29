/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>BeginWindowTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class BeginWindowTuple extends WindowIdTuple
{
  private static final byte[][] serializedTuples = new byte[16000][];

  static {
    for (int i = serializedTuples.length; i-- > 0;) {
      serializedTuples[i] = WindowIdTuple.getSerializedTuple(i);
      serializedTuples[i][0] = MessageType.BEGIN_WINDOW_VALUE;
    }
  }

  public BeginWindowTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedTuple(int windowId)
  {
    return serializedTuples[windowId % serializedTuples.length];
  }

}
