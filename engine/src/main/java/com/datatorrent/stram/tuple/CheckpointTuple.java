/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 * <p>CheckpointTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class CheckpointTuple extends Tuple
{
  public CheckpointTuple(long windowId)
  {
    super(MessageType.CHECKPOINT, windowId);
  }
}

