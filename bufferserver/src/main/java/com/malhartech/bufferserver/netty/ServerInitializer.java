/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.netty;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ServerHandler;
import com.malhartech.bufferserver.util.SerializedData;
import io.netty.buffer.ByteBuf;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class ServerInitializer extends ChannelInitializer<SocketChannel>
{
    private static final Logger logger = LoggerFactory.getLogger(ServerInitializer.class);
    private final ServerHandler serverHandler;
    private final ProtobufDecoder protobufDecoder;
    private final MessageToMessageEncoder<SerializedData, ByteBuf> encoder;

    /**
     *
     */
    public ServerInitializer(int buffersize, int blockcount)
    {
        serverHandler = new ServerHandler(buffersize, blockcount);
        protobufDecoder = new ProtobufDecoder(Buffer.Message.getDefaultInstance());
        encoder = new MessageToMessageEncoder<SerializedData, ByteBuf>()
        {
            @Override
            public ByteBuf encode(ChannelHandlerContext ctx, SerializedData msg) throws Exception
            {
                return wrappedBuffer(msg.bytes, msg.offset, msg.size);
            }
        };
    }

    /**
     *
     * @param channel
     * @throws Exception
     */
    @Override
    public void initChannel(SocketChannel channel) throws Exception
    {
        ChannelPipeline p = channel.pipeline();
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", protobufDecoder);
        p.addLast("encoder", encoder);
        p.addLast("handler", serverHandler);
    }
}