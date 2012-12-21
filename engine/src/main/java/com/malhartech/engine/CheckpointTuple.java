/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.bufferserver.Buffer.Message.MessageType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CheckpointTuple extends Tuple
{
  public CheckpointTuple()
  {
    super(MessageType.CHECKPOINT);
  }
}

