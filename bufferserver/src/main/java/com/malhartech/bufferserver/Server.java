/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ServerInitializer;
import com.malhartech.bufferserver.storage.Storage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The buffer server application<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Server
{
  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  public static final int DEFAULT_PORT = 9080;
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
  public static final int DEFAULT_BLOCK_COUNT = 8;
  private final int port;
  private ServerBootstrap bootstrap;
  private String identity;
  private final ServerInitializer serverInitializer;
  private Storage storage;

  /**
   * @param port - port number to bind to or 0 to auto select a free port
   */
  public Server(int port)
  {
    this(port, DEFAULT_BUFFER_SIZE, DEFAULT_BLOCK_COUNT);
  }

  public Server(int port, int blocksize, int blockcount)
  {
    this.port = port;
    serverInitializer = new ServerInitializer(blocksize, blockcount);
  }

  public void setSpoolStorage(Storage storage)
  {
    serverInitializer.setSpoolStorage(storage);
  }
  /**
   *
   * @return {@link java.net.SocketAddress}
   * @throws Exception
   */
  public SocketAddress run() throws Exception
  {
    // Configure the server.
    bootstrap = new ServerBootstrap();

    bootstrap.group(new NioEventLoopGroup(), new NioEventLoopGroup())
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 100)
            .localAddress(port)
//            .childOption(ChannelOption.ALLOW_HALF_CLOSURE, true)
            .childHandler(serverInitializer);

    ChannelFuture f = bootstrap.bind().syncUninterruptibly();
    identity = f.channel().localAddress().toString();
    logger.info("Server instance bound to: {}", identity);

    return f.channel().localAddress();
  }

  /**
   *
   */
  public void shutdown()
  {
    logger.info("Server instance {} being shutdown", identity);
    bootstrap.shutdown();
  }

  /**
   *
   * @param args
   * @throws Exception
   */
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

  @Override
  public String toString()
  {
    return identity;
  }
}
