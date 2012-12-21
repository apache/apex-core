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

/**
 * Extend {@link io.netty.channel.ChannelInitializer}
 *
 */
public class ClientInitializer extends ChannelInitializer<SocketChannel>
{
  private static final Logger logger = LoggerFactory.getLogger(ClientInitializer.class);
  final ChannelHandler handler;

  /**
   *
   * @param handler
   */
  public ClientInitializer(ChannelHandler handler)
  {
    this.handler = handler;
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
    p.addLast("protobufDecoder", new ProtobufDecoder(Buffer.Message.getDefaultInstance()));

    p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
    p.addLast("protobufEncoder", new ProtobufEncoder());
    //    p.addLast("debug", new SimpleChannelDownstreamHandler()
    //    {
    //
    //      @Override
    //      public void writeRequested(ChannelHandlerContext ctx, MessageEvent me) throws Exception
    //      {
    //        logger.debug("buffer handing out {0} for window {1}", new Object[]{((Message) me.getMessage()).getType(), ((Message) me.getMessage()).getWindowId()});
    //        super.writeRequested(ctx, me);
    //      }
    //    });

    p.addLast("handler", handler);
  }
}
