/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Message;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Sharable
public class BufferServerController extends AbstractSocketSubscriber<Buffer.Message>
{
  private final String sourceId;
  long windowId;
  Message data;

  public BufferServerController(String sourceId)
  {
    this.sourceId = sourceId;
  }

  @Override
  public void activate()
  {
    super.activate();
  }

  public void purge()
  {
    data = null;
    ClientHandler.purge(channel, sourceId, windowId);
  }

  public void reset()
  {
    data = null;
    ClientHandler.reset(channel, sourceId, windowId);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, Message data) throws Exception
  {
//    logger.debug("received {}", data);
    this.data = data;
  }
}
