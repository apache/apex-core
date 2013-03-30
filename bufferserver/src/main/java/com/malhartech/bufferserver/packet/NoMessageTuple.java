/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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
