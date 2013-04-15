/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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
