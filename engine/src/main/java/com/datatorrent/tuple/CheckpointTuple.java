/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CheckpointTuple extends Tuple
{
  public CheckpointTuple(long windowId)
  {
    super(MessageType.CHECKPOINT, windowId);
  }
}

