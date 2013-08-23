/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.tuple;

import com.datatorrent.bufferserver.packet.MessageType;

/**
 *
 * ResetWindow ids<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
