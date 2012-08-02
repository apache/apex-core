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
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;

public class ServerPipelineFactory implements ChannelPipelineFactory
{
  public static final Logger logger = Logger.getLogger(ServerPipelineFactory.class.getName());
  private ServerHandler serverHandler = new ServerHandler();
  private ProtobufDecoder protobufDecoder = new ProtobufDecoder(Buffer.Data.getDefaultInstance());
//  private ProtobufEncoder protobufEncoder = new ProtobufEncoder();
//  private ProtobufVarint32LengthFieldPrepender lengthPrepender = new ProtobufVarint32LengthFieldPrepender();
  private OneToOneEncoder encoder = new OneToOneEncoder()
  {
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception
    {
      return wrappedBuffer(((SerializedData) msg).bytes, ((SerializedData) msg).offset, ((SerializedData) msg).size);
    }
  };

  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline p = pipeline();
    p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
    p.addLast("protobufDecoder", protobufDecoder);

    //p.addLast("frameEncoder", lengthPrepender);
    //p.addLast("protobufEncoder", protobufEncoder);

//    p.addLast("debug", new SimpleChannelDownstreamHandler() {
//      @Override
//      public void writeRequested(ChannelHandlerContext ctx, MessageEvent me) throws Exception {
//        logger.log(Level.INFO, "buffer handing out {0} for window {1}", new Object[]{((Data)me.getMessage()).getType(), ((Data)me.getMessage()).getWindowId()});
//        super.writeRequested(ctx, me);        
//      }
//    });
    p.addLast("encoder", encoder);
    p.addLast("handler", serverHandler);
    return p;
  }
}