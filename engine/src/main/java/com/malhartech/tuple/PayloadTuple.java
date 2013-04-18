/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.tuple;

import com.malhartech.bufferserver.packet.MessageType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PayloadTuple extends Tuple
{
  final Object payload;
  public PayloadTuple(Object payload)
  {
    super(MessageType.PAYLOAD);
    this.payload = payload;
  }

  public Object getPayload()
  {
    return payload;
  }
}
