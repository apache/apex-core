/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.netty.ClientInitializer;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Stream class and provides basic stream connection for a node to write to a socket<p>
 * <br>
 * Most likely users would not use it to write to a socket by themselves. Is used in adapters and by BufferServerOutputStream<br>
 * <br>
 *
 * @author chetan
 */
@Sharable
public abstract class SocketOutputStream<T> extends ChannelOutboundMessageHandlerAdapter<Object> implements Stream<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(SocketOutputStream.class);
  protected Bootstrap bootstrap;
  protected Channel channel;

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext context)
  {
    bootstrap = new Bootstrap();
    bootstrap.group(new NioEventLoopGroup())
            .channel(NioSocketChannel.class)
            .remoteAddress(context.getBufferServerAddress())
            .handler(new ClientInitializer(this));
    channel = bootstrap.connect().syncUninterruptibly().channel();
  }

  @Override
  public void flush(ChannelHandlerContext ctx, ChannelFuture future) throws Exception
  {
    ctx.outboundMessageBuffer().drainTo(ctx.nextOutboundMessageBuffer());
    ctx.flush(future);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
  {
    if (!(cause instanceof java.nio.channels.ClosedChannelException)) {
      super.exceptionCaught(ctx, cause);
    }
  }

  @Override
  public void deactivate()
  {
    if (channel != null) {
      channel.flush().awaitUninterruptibly();
      channel.close().awaitUninterruptibly();
    }
    bootstrap.shutdown();
  }
}
