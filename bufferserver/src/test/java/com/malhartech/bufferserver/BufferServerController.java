/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

import com.malhartech.bufferserver.Buffer.Data;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Sharable
public class BufferServerController extends AbstractSocketSubscriber<Buffer.Data>
{
  private final String sourceId;
  long windowId;
  Data data;

  public BufferServerController(String sourceId)
  {
    this.sourceId = sourceId;
  }

  @Override
  public void activate()
  {
    data = null;
    super.activate();
    ClientHandler.purge(channel, sourceId, windowId);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, Data data) throws Exception
  {
//    logger.debug("received {}", data);
    this.data = data;
  }
}
