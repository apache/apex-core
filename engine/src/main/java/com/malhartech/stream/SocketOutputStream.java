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
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
 * @author chetan
 */
public class SocketOutputStream extends ChannelOutboundMessageHandlerAdapter implements Stream
{
    private static Logger logger = LoggerFactory.getLogger(SocketOutputStream.class);
    protected StreamContext context;
    protected Bootstrap bootstrap;
    protected Channel channel;

    @Override
    public void setup(StreamConfiguration config)
    {
        bootstrap = new Bootstrap();

        bootstrap.group(new NioEventLoopGroup())
                .channel(new NioSocketChannel())
                .remoteAddress(config.getBufferServerAddress())
                .handler(new ClientInitializer(this.getClass()));
    }

    @Override
    public void setContext(StreamContext context)
    {
        this.context = context;
    }

    @Override
    public StreamContext getContext()
    {
        return context;
    }

    @Override
    public void teardown()
    {
        channel.close().awaitUninterruptibly();
        bootstrap.shutdown();
    }

    @Override
    public void activate()
    {
        channel = bootstrap.connect().syncUninterruptibly().channel();
    }

    @Override
    public void flush(ChannelHandlerContext ctx, ChannelFuture future) throws Exception
    {
        ctx.outboundMessageBuffer().drainTo(ctx.nextOutboundMessageBuffer());
        ctx.flush(future);
    }
}
