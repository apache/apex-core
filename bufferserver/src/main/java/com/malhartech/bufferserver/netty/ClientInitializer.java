/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.netty;

import com.malhartech.bufferserver.Buffer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientInitializer extends ChannelInitializer<SocketChannel>
{
    private static final Logger logger = LoggerFactory.getLogger(ClientInitializer.class);
    final Class<? extends ChannelHandler> handler;

    public ClientInitializer(Class<? extends ChannelHandler> handler)
    {
        this.handler = handler;
    }

    @Override
    public void initChannel(SocketChannel channel) throws Exception
    {
        ChannelPipeline p = channel.pipeline();
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(Buffer.Data.getDefaultInstance()));

        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());
//    p.addLast("debug", new SimpleChannelDownstreamHandler()
//    {
//
//      @Override
//      public void writeRequested(ChannelHandlerContext ctx, MessageEvent me) throws Exception
//      {
//        logger.debug("buffer handing out {0} for window {1}", new Object[]{((Data) me.getMessage()).getType(), ((Data) me.getMessage()).getWindowId()});
//        super.writeRequested(ctx, me);
//      }
//    });

        p.addLast("handler", handler.newInstance());
    }
}
