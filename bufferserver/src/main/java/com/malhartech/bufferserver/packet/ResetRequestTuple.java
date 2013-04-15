/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ResetRequestTuple extends GenericRequestTuple
{
  public ResetRequestTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedRequest(String identifier, long startingWindowId)
  {
    return GenericRequestTuple.getSerializedRequest(identifier, startingWindowId, MessageType.RESET_REQUEST_VALUE);
  }

}
