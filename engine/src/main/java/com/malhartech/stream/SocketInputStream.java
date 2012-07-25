/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.netty.ClientPipelineFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelLocal;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 *
 * @author chetan
 */
public class SocketInputStream extends SimpleChannelUpstreamHandler implements Stream
{
  private static final Logger logger = Logger.getLogger(ClientHandler.class.getName());
  public static final ChannelLocal<StreamContext> contexts = new ChannelLocal<StreamContext>()
  {
    @Override
    protected StreamContext initialValue(Channel channel)
    {
      return null;
    }
  };
  protected Channel channel;
  private ClientBootstrap bootstrap;
  private InetSocketAddress serverAddress;
  private StreamContext context;

  protected ClientPipelineFactory getClientPipelineFactory()
  {
    return new ClientPipelineFactory(this.getClass());
  }

  @Override
  public void setup(StreamConfiguration config)
  {
    bootstrap = new ClientBootstrap(
      new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()));

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(getClientPipelineFactory());

    serverAddress = config.getBufferServerAddress();
  }

  @Override
  public void setContext(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
//    contexts.set(channel, context);
  }

  @Override
  public void teardown()
  {
    contexts.remove(channel);
    channel.close();
    channel.getCloseFuture().awaitUninterruptibly();
    bootstrap.releaseExternalResources();
  }

  @Override
  public StreamContext getContext()
  {
    //return context;
    return contexts.get(channel);
  }

  public void activate()
  {
    // Make a new connection.
    ChannelFuture future = bootstrap.connect(serverAddress);
    channel = future.awaitUninterruptibly().getChannel();
    contexts.set(channel, context);
  }
}
