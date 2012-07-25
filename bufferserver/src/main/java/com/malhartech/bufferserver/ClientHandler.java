/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.channel.*;

/**
 * this class is called the last while reading the response from server.
 *
 * @author chetan
 */
public class ClientHandler extends SimpleChannelUpstreamHandler
{

  private static final Logger logger = Logger.getLogger(
          ClientHandler.class.getName());
  // Stateful properties
  private volatile Channel channel;

  public static void publish(Channel channel, String identifier, String type, long windowId)
  {
    Buffer.PublisherRequest.Builder prb = Buffer.PublisherRequest.newBuilder();
    prb.setIdentifier(identifier).setType(type);

    Data.Builder db = Data.newBuilder();
    db.setType(Data.DataType.PUBLISHER_REQUEST);
    db.setPublish(prb);
    db.setWindowId(windowId);
    
    final ChannelFutureListener cfl = new ChannelFutureListener()
    {

      public void operationComplete(ChannelFuture cf) throws Exception
      {
        Buffer.PartitionedData.Builder pdb = Buffer.PartitionedData.newBuilder();
        pdb.setData(ByteString.EMPTY);

        byte[] bytes = String.valueOf(new Random().nextInt() % 10).getBytes();
        pdb.setPartition(ByteString.copyFrom(bytes));


        Buffer.Data.Builder db = Data.newBuilder();
        db.setType(Data.DataType.PARTITIONED_DATA);
        db.setWindowId(new Date().getTime());
        db.setPartitioneddata(pdb);

        Thread.sleep(500);
        cf.getChannel().write(db).addListener(this);
      }
    };

    channel.write(db.build());//.addListener(cfl);
  }

  public static void registerPartitions(Channel channel, String id,
                                 String down_type,
                                 String node,
                                 String type,
                                 Collection<byte[]> partitions)
  {
    Buffer.SubscriberRequest.Builder srb = Buffer.SubscriberRequest.newBuilder();
    srb.setIdentifier(id);
    srb.setType(down_type);
    srb.setUpstreamIdentifier(node);
    srb.setUpstreamType(type);

    if (partitions != null) {
      for (byte[] c : partitions) {
        srb.addPartition(ByteString.copyFrom(c));
      }
    }

    Data.Builder builder = Data.newBuilder();
    builder.setType(Data.DataType.SUBSCRIBER_REQUEST);
    builder.setSubscribe(srb);
    builder.setWindowId(0L); // TODO Message missing required fields: window_id
    
    channel.write(builder.build());
  }

  @Override
  public void handleUpstream(
          ChannelHandlerContext ctx, ChannelEvent e) throws Exception
  {
    if (e instanceof ChannelStateEvent) {
      logger.info(e.toString());
    }

    System.out.println(e);

    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
          throws Exception
  {
    channel = e.getChannel();
    super.channelOpen(ctx, e);
  }

  @Override
  public void messageReceived(
          ChannelHandlerContext ctx, final MessageEvent e)
  {
    Data data = (Data) e.getMessage();
    System.out.println(data.getType());
  }

  @Override
  public void exceptionCaught(
          ChannelHandlerContext ctx, ExceptionEvent e)
  {
    logger.log(
            Level.WARNING,
            "Unexpected exception from downstream.",
            e.getCause());
    e.getChannel().close();
  }

  @Override
  public void channelDisconnected(ChannelHandlerContext ctx,
                                  ChannelStateEvent e) throws Exception
  {
    //compiled code
    throw new RuntimeException("Compiled Code");
  }
}
