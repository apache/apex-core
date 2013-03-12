/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.Buffer.Message.MessageType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Tuple
{
  public MessageType getType();

  public long getWindowId();

  public int getIntervalMillis();

  public int getBaseSeconds();
}
