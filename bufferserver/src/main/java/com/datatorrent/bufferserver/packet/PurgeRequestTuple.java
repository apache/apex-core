/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>PurgeRequestTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
