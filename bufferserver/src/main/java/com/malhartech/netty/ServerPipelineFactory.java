/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.netty;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.ServerHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.jboss.netty.channel.Channels.pipeline;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

public class ServerPipelineFactory implements ChannelPipelineFactory
{
  public static final Logger logger = Logger.getLogger(ServerPipelineFactory.class.getName());

  private ServerHandler sh = new ServerHandler();

  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline p = pipeline();
    p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
    p.addLast("protobufDecoder", new ProtobufDecoder(Buffer.Data.getDefaultInstance()));

    p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
    p.addLast("protobufEncoder", new ProtobufEncoder());
    
//    p.addLast("debug", new SimpleChannelDownstreamHandler() {
//      @Override
//      public void writeRequested(ChannelHandlerContext ctx, MessageEvent me) throws Exception {
//        logger.log(Level.INFO, "buffer handing out {0} for window {1}", new Object[]{((Data)me.getMessage()).getType(), ((Data)me.getMessage()).getWindowId()});
//        super.writeRequested(ctx, me);        
//      }
//    });
    p.addLast("handler", getSh());
    return p;
  }

  /**
   * @return the sh
   */
  public ServerHandler getSh()
  {
    return sh;
  }

  /**
   * @param sh the sh to set
   */
  public void setSh(ServerHandler sh)
  {
    this.sh = sh;
  }
}