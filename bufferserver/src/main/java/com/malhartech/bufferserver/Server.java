/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ServerPipelineFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Server
{
  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  public static final int DEFAULT_PORT = 9080;
  private DefaultChannelGroup allConnected = new DefaultChannelGroup("all-connected");
  private final int port;
  private ServerBootstrap bootstrap;

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this.port = port;
  }

  public SocketAddress run()
  {
    // Configure the server.
    bootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool()));

    // Set up the event pipeline factory.
    bootstrap.setPipelineFactory(new ServerPipelineFactory(allConnected));

    // Bind and start to accept incoming connections.
    Channel serverChannel = bootstrap.bind(new InetSocketAddress(port));
    logger.info("Server instance {} bound to: {}", this, serverChannel.getLocalAddress());

    allConnected.add(serverChannel);

    return serverChannel.getLocalAddress();
  }

  public void shutdown()
  {
    logger.info("Server instance {} being shutdown with connections {}", this, allConnected);
    allConnected.close().awaitUninterruptibly();
    this.bootstrap.releaseExternalResources();
  }

  public static void main(String[] args) throws Exception
  {
    int port;
    if (args.length > 0) {
      port = Integer.parseInt(args[0]);
    }
    else {
      port = DEFAULT_PORT;
    }
    new Server(port).run();
  }
}