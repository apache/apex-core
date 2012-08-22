/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stream;

import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.bufferserver.netty.ClientPipelineFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */

/**
 * Implements Stream class and provides basic stream connection for a node to write to a socket<p>
 * <br>
 * Most likely users would not use it to write to a socket by themselves. Is used in adapters and by BufferServerOutputStream<br>
 * <br>
 */

public class SocketOutputStream extends SimpleChannelDownstreamHandler implements Stream
{
  private static Logger logger = LoggerFactory.getLogger(SocketOutputStream.class);
  protected StreamContext context;
  protected ClientBootstrap bootstrap;
  protected Channel channel;
  private InetSocketAddress serverAddress;

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(this.getClass());
  }

  @Override
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                        Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(getClientPipelineFactory());

    serverAddress = config.getBufferServerAddress();
  }

  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
    // send publisher request
  }

  @Override
  public StreamContext getContext()
  {
    return context;
  }

  @Override
  public void teardown()
  {
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }

  @Override
  public void activate()
  {
    // Make a new connection.
    ChannelFuture future = bootstrap.connect(serverAddress);
    channel = future.awaitUninterruptibly().getChannel();
  }
}
