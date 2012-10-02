/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractSocketPublisher extends ChannelOutboundMessageHandlerAdapter
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSocketPublisher.class);
  protected Bootstrap bootstrap;
  protected Channel channel;

  public void setup(String host, int port)
  {
    bootstrap = new Bootstrap();

    bootstrap.group(new NioEventLoopGroup())
            .channel(new NioSocketChannel())
            .remoteAddress(host, port)
            .handler(new ClientInitializer(this));
  }

  public void teardown()
  {
    bootstrap.shutdown();
  }

  public void activate()
  {
    channel = bootstrap.connect().syncUninterruptibly().channel();
  }

  public void flush(ChannelHandlerContext ctx, ChannelFuture future) throws Exception
  {
    ctx.outboundMessageBuffer().drainTo(ctx.nextOutboundMessageBuffer());
    ctx.flush(future);
  }

  public void deactivate()
  {
    channel.close().awaitUninterruptibly();
  }
}