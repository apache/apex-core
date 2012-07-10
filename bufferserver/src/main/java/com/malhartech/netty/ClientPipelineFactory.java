/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.netty;

import com.malhartech.bufferserver.Buffer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class ClientPipelineFactory implements ChannelPipelineFactory
{

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

    p.addLast("handler", handler.newInstance());
    return p;
  }
}
