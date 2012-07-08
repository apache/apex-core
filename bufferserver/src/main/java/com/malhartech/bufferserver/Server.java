/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.malhartech.netty.ServerPipelineFactory;

/**
 * Receives a list of continent/city pairs from a {@link LocalTimeClient} to get
 * the local times of the specified cities.
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

public class Server
{
  private static final Logger LOGGER = Logger.getLogger(Server.class.getName());
  
  public static int DEFAULT_PORT = 9080;
  private final int port;

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
    ServerBootstrap bootstrap = new ServerBootstrap(
            new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()));

    // Set up the event pipeline factory.
    bootstrap.setPipelineFactory(new ServerPipelineFactory());

    // Bind and start to accept incoming connections.
    Channel c = bootstrap.bind(new InetSocketAddress(port));
    LOGGER.info("bound to : " + c.getLocalAddress());
    return c.getLocalAddress();
  }

  public static void main(String[] args) throws Exception
  {
    Handler ch = new ConsoleHandler();
    Logger.getLogger("").addHandler(ch);

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