/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import static com.malhartech.bufferserver.packet.Tuple.VERSION;
import com.malhartech.bufferserver.util.Codec;
import java.util.Arrays;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PublishRequestTuple extends GenericRequestTuple
{
  public PublishRequestTuple(byte[] array, int offset, int len)
  {
    super(array, offset, len);
  }

  public static byte[] getSerializedRequest(String identifier, long startingWindowId)
  {
    return GenericRequestTuple.getSerializedRequest(identifier, startingWindowId, MessageType.PUBLISHER_REQUEST_VALUE);
  }

}
