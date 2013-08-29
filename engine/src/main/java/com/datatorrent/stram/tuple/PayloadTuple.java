/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * <p>PayloadTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
