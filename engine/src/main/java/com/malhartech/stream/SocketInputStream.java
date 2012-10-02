/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.netty.ClientInitializer;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
/**
 *
 * Implements a stream that is read from a socket by a node<p>
 * <br>
 * The main class for all socket based input streams.<br>
 * <br>
 *
 */
@Sharable
public abstract class SocketInputStream<T> extends ChannelInboundMessageHandlerAdapter<T> implements Stream
{
  private static final Logger logger = LoggerFactory.getLogger(SocketInputStream.class);
  protected Channel channel;
  private Bootstrap bootstrap;

  @Override
  public void setup(StreamConfiguration config)
  {
    bootstrap = new Bootstrap();

    bootstrap.group(new NioEventLoopGroup())
            .channel(NioSocketChannel.class)
            .remoteAddress(config.getBufferServerAddress())
            .handler(new ClientInitializer(this));
  }

  @Override
  public void teardown()
  {
    bootstrap.shutdown();
  }

  @Override
  public void activate(StreamContext context)
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

  @Override
  public void deactivate()
  {
    channel.close().awaitUninterruptibly();
  }
}
