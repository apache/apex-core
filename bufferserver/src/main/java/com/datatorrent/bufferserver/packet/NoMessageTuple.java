/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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
