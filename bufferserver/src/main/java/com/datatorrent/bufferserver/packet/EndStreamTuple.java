/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>EndStreamTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class EndStreamTuple extends WindowIdTuple
{
  public static byte[] getSerializedTuple(int windowId)
  {
    byte[] serializedTuple = WindowIdTuple.getSerializedTuple(windowId);
    serializedTuple[0] = MessageType.END_STREAM_VALUE;
    return serializedTuple;
  }

  public EndStreamTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

}
