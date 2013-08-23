/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * <p>CodecStateTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
