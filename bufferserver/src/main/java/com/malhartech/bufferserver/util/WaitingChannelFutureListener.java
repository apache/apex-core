/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.util;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WaitingChannelFutureListener implements ChannelFutureListener
{
  volatile boolean added;

  @Override
  public synchronized void operationComplete(ChannelFuture future) throws Exception
  {
    added = false;
    notify();
  }

  public void setAdded(boolean flag)
  {
    added = flag;
  }

  public boolean isAdded()
  {
    return added;
  }
}
