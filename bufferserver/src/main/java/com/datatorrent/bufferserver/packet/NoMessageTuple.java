/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>NoMessageTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class NoMessageTuple extends EmptyTuple
{
  public NoMessageTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
  }

  public static byte[] getSerializedTuple()
  {
    return EmptyTuple.getSerializedTuple(MessageType.NO_MESSAGE_VALUE);
  }

}
