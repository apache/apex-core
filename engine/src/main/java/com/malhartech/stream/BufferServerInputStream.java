/**
 * Copyright (c) 2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.*;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Implement tuple flow from buffer server to the node in a logical stream<p>
  * <br>
  * Extends SocketInputStream as buffer server and node communicate via a socket<br>
  * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
  */

public class BufferServerInputStream extends SocketInputStream
{
  private static Logger logger = LoggerFactory.getLogger(BufferServerInputStream.class);
  private long baseSeconds = 0;

  @Override
  public void activate()
  {
    super.activate();

    BufferServerStreamContext sc = (BufferServerStreamContext) getContext();
    String type = "paramNotRequired?"; // TODO: why do we need this?
    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={}", new Object[]{sc.getSinkId(), sc.getSourceId(), sc.getId()});
    ClientHandler.registerPartitions(channel, sc.getSinkId(), sc.getId() + '/' + sc.getSinkId(), sc.getSourceId(), type, sc.getPartitions(), sc.getStartingWindowId());
  }

  // most of this code should be abstracted to the SocketInputStream and just 
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception
  {
//    logger.debug("received message {}", e.getMessage());
    StreamContext context = contexts.get(ctx.getChannel());
    if (context == null) {
      logger.warn("Context is not setup for the InputSocketStream");
    }
    else {
      Buffer.Data d = (Buffer.Data) e.getMessage();

      Tuple t;
      switch (d.getType()) {
        case SIMPLE_DATA:
          t = new Tuple(context.getSerDe().fromByteArray(d.getSimpleData().getData().toByteArray()));
          t.setType(Buffer.Data.DataType.SIMPLE_DATA);
          t.setWindowId(baseSeconds | d.getWindowId());
          break;

        case PARTITIONED_DATA:
          t = new Tuple(context.getSerDe().fromByteArray(d.getPartitionedData().getData().toByteArray()));
          /*
           * we really do not distinguish between SIMPLE_DATA and PARTITIONED_DATA
           */
          t.setType(Buffer.Data.DataType.SIMPLE_DATA);
          t.setWindowId(baseSeconds | d.getWindowId());
          break;

        case END_WINDOW:
          t = new EndWindowTuple();
          t.setWindowId(baseSeconds | d.getWindowId());
          break;

        case END_STREAM:
          t = new EndStreamTuple();
          t.setWindowId(baseSeconds | d.getWindowId());
          break;

        case RESET_WINDOW:
          t = new ResetWindowTuple();
          baseSeconds = (long) d.getWindowId() << 32;
          t.setWindowId(baseSeconds | d.getResetWindow().getWidth());
          break;

        default:
          t = new Tuple(null);
          t.setType(d.getType());
          t.setWindowId(baseSeconds | d.getWindowId());
          break;
      }

      context.sink(t);
    }
  }
}