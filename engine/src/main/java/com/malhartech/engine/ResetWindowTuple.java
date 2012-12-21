/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.bufferserver.Buffer;

/**
 *
 * ResetWindow ids<p>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ResetWindowTuple extends Tuple
{
  public ResetWindowTuple()
  {
    super(Buffer.Message.MessageType.RESET_WINDOW);
  }

  @Override
  public final long getWindowId()
  {
    return super.windowId & 0xffffffff00000000L;
  }

  public final int getBaseSeconds()
  {
    return (int) (super.windowId >> 32);
  }

  public final int getIntervalMillis()
  {
    return (int) super.windowId;
  }
}
