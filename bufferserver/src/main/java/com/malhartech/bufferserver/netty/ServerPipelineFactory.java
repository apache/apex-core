/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.netty;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ServerHandler;
import com.malhartech.bufferserver.util.SerializedData;
import java.util.logging.Logger;
import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

public class ServerPipelineFactory implements ChannelPipelineFactory
{
  public static final Logger logger = Logger.getLogger(ServerPipelineFactory.class.getName());
  private final ServerHandler serverHandler;
  private final ProtobufDecoder protobufDecoder;
  private final OneToOneEncoder encoder;

  public ServerPipelineFactory(ChannelGroup connectedChannels)
  {
    serverHandler = new ServerHandler(connectedChannels);
    protobufDecoder = new ProtobufDecoder(Buffer.Data.getDefaultInstance());
    encoder = new OneToOneEncoder()
    {
      @Override
      protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception
      {
        return wrappedBuffer(((SerializedData) msg).bytes, ((SerializedData) msg).offset, ((SerializedData) msg).size);
      }
    };
  }

  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline p = pipeline();
    p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
    p.addLast("protobufDecoder", protobufDecoder);
    p.addLast("encoder", encoder);
    p.addLast("handler", serverHandler);
    return p;
  }
}