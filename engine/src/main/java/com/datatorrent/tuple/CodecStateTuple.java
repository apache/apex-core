/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class CodecStateTuple extends Tuple
{
  public final byte[] state;

  public CodecStateTuple(long windowId, byte[] state)
  {
    super(MessageType.CODEC_STATE);
    this.windowId = windowId;
    this.state = state;
  }
}
