/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.bufferserver.netty.ClientInitializer;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
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

public abstract class SocketInputStream<T> extends ChannelInboundMessageHandlerAdapter<T> implements Stream
{
    private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);
    protected static final AttributeKey<StreamContext> CONTEXT = new AttributeKey<StreamContext>("context");
    protected Channel channel;
    private Bootstrap bootstrap;

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
    public void teardown()
    {
        channel.attr(CONTEXT).remove();
        channel.close().awaitUninterruptibly();
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

//    channel.getConfig().setBufferFactory(new ChannelBufferFactory() {
//      @SuppressWarnings("PackageVisibleField")
//      DirectChannelBufferFactory impl = new DirectChannelBufferFactory(1024 * 1024);
//
//      @Override
//      public ChannelBuffer getBuffer(int i)
//      {
//        logger.debug(String.valueOf(i));
//        return impl.getBuffer(i);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteOrder bo, int i)
//      {
//        logger.debug(bo + ", " + i);
//        return impl.getBuffer(bo, i);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(byte[] bytes, int i, int i1)
//      {
//        logger.debug(bytes + ", " + i + " " + i1);
//        return impl.getBuffer(bytes, i, i1);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteOrder bo, byte[] bytes, int i, int i1)
//      {
//        logger.debug(bo + ", " + bytes + ", " + i + " " + i1);
//        return impl.getBuffer(bytes, i, i1);
//      }
//
//      @Override
//      public ChannelBuffer getBuffer(ByteBuffer bb)
//      {
//        logger.debug("" + bb);
//        return impl.getBuffer(bb);
//      }
//
//      @Override
//      public ByteOrder getDefaultOrder()
//      {
//        ByteOrder bo = impl.getDefaultOrder();
//        logger.debug("" + bo);
//        return bo;
//      }
//    });
        channel.attr(CONTEXT).set(context);
    }
}
