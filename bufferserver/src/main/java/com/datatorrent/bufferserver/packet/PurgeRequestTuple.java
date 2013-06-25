/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class PurgeRequestTuple extends GenericRequestTuple
{
  public PurgeRequestTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public static byte[] getSerializedRequest(String version, String id, long windowId)
  {
    return GenericRequestTuple.getSerializedRequest(version, id, windowId, MessageType.PURGE_REQUEST_VALUE);
  }

}
