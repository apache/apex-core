/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.tuple;

import com.malhartech.bufferserver.packet.MessageType;

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

