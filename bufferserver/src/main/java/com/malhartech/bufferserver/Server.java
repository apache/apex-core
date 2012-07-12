/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.netty.ServerPipelineFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

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
  private ServerBootstrap bootstrap;
  private Channel serverChannel;
  
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
    bootstrap.setPipelineFactory(new ServerPipelineFactory());

    // Bind and start to accept incoming connections.
    serverChannel = bootstrap.bind(new InetSocketAddress(port));
    LOGGER.log(Level.INFO, "bound to: {0}", serverChannel.getLocalAddress());

    return serverChannel.getLocalAddress();
  }

  public void shutdown() {
    if (serverChannel !=  null) {
      LOGGER.log(Level.INFO, "shutting down server at: {0}", serverChannel.getLocalAddress());
      this.serverChannel.close();
      //this.bootstrap.releaseExternalResources();
      this.serverChannel =  null;
    }
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