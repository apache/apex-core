/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

/**
 * <p>PublishRequestTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class PublishRequestTuple extends GenericRequestTuple
{
  public PublishRequestTuple(byte[] array, int offset, int len)
  {
    super(array, offset, len);
  }

  public static byte[] getSerializedRequest(String version, String identifier, long startingWindowId)
  {
    return GenericRequestTuple.getSerializedRequest(version, identifier, startingWindowId, MessageType.PUBLISHER_REQUEST_VALUE);
  }

}
