/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
@Sharable
public class BufferServerController extends AbstractSocketSubscriber<Buffer.Data>
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerController.class);
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
