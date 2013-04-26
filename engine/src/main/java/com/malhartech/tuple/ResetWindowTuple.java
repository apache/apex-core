/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.tuple;

import com.malhartech.bufferserver.packet.MessageType;

/**
 *
 * ResetWindow ids<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ResetWindowTuple extends Tuple
{
  public ResetWindowTuple(long windowId)
  {
    super(MessageType.RESET_WINDOW);
    this.windowId = windowId;
  }

  @Override
  public final long getWindowId()
  {
    return super.windowId & 0xffffffff00000000L;
  }

  public final int getBaseSeconds()
  {
    return (int)(super.windowId >> 32);
  }

  public final int getIntervalMillis()
  {
    return (int)super.windowId;
  }

}
