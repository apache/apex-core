/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.netty;

import com.malhartech.bufferserver.Buffer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.LoggerFactory;

public class ClientPipelineFactory implements ChannelPipelineFactory
{

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ClientPipelineFactory.class);
  final Class<? extends ChannelHandler> handler;

  public ClientPipelineFactory(Class<? extends ChannelHandler> handler)
  {
    this.handler = handler;
  }

  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline p = pipeline();
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
    return p;
  }
}
