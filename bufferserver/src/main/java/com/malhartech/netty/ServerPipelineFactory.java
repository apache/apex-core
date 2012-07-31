/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.netty;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ServerHandler;
import java.util.logging.Logger;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class ServerPipelineFactory implements ChannelPipelineFactory
{
  public static final Logger logger = Logger.getLogger(ServerPipelineFactory.class.getName());

  private ServerHandler serverHandler = new ServerHandler();
  private ProtobufDecoder protobufDecoder = new ProtobufDecoder(Buffer.Data.getDefaultInstance());
  private ProtobufEncoder protobufEncoder = new ProtobufEncoder();
  private ProtobufVarint32LengthFieldPrepender lengthPrepender = new ProtobufVarint32LengthFieldPrepender();

  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline p = pipeline();
    p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
    p.addLast("protobufDecoder", protobufDecoder);

    p.addLast("frameEncoder", lengthPrepender);
    p.addLast("protobufEncoder", protobufEncoder);
    
//    p.addLast("debug", new SimpleChannelDownstreamHandler() {
//      @Override
//      public void writeRequested(ChannelHandlerContext ctx, MessageEvent me) throws Exception {
//        logger.log(Level.INFO, "buffer handing out {0} for window {1}", new Object[]{((Data)me.getMessage()).getType(), ((Data)me.getMessage()).getWindowId()});
//        super.writeRequested(ctx, me);        
//      }
//    });
    p.addLast("handler", serverHandler);
    return p;
  }
}