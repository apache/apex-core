/*
 * Copyright (c) 2012 Malhar, Inc.
 * All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ServerInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.SocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Server
{
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    public static final int DEFAULT_PORT = 9080;
    private final int port;
    private ServerBootstrap bootstrap;

    /**
     * @param port - port number to bind to or 0 to auto select a free port
     */
    public Server(int port)
    {
        this.port = port;
    }

    public SocketAddress run() throws Exception
    {
        // Configure the server.
        bootstrap = new ServerBootstrap();

        bootstrap.group(new NioEventLoopGroup(), new NioEventLoopGroup())
                .channel(new NioServerSocketChannel())
                .option(ChannelOption.SO_BACKLOG, 100)
                .localAddress(port)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ServerInitializer());

        ChannelFuture f = bootstrap.bind().syncUninterruptibly();

        logger.info("Server instance {} bound to: {}", this, f.channel().localAddress());

        return f.channel().localAddress();
    }

    public void shutdown()
    {
        logger.info("Server instance {} being shutdown", this);
        bootstrap.shutdown();
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
