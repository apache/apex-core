/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractSocketSubscriber<T> extends ChannelInboundMessageHandlerAdapter<T>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSocketSubscriber.class);
  protected Channel channel;
  private Bootstrap bootstrap;

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
    // Make a new connection.
    channel = bootstrap.connect().syncUninterruptibly().channel();

    // Netty does not provide a way to read in all the data that comes
    // onto the channel into a byte buffer managed by the user. It causes
    // various problems:
    // 1. There is excessive copy of data between the 2 buffers.
    // 2. Once the BufferFactory has given out the buffer, it does not know
    //    if it can ever recycle it.
    // 3. Causes fragmentation and need for garbage collection

    // Netty needs some way to prevent it.
  }

  public void deactivate()
  {
    channel.close().awaitUninterruptibly();
  }

}
