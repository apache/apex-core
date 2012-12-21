/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.*;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is called the last while reading the response from server<p>
 * <br>
 *
 * @author chetan
 */
@Sharable
public class ClientHandler extends ChannelInboundMessageHandlerAdapter<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

  /**
   *
   * @param channel
   * @param identifier
   * @param type
   * @param startingWindowId
   */
  public static void publish(Channel channel, String identifier, String type, long startingWindowId)
  {
    Buffer.PublisherRequest.Builder prb = Buffer.PublisherRequest.newBuilder();
    prb.setIdentifier(identifier).setBaseSeconds((int)(startingWindowId >> 32));


    Message.Builder db = Message.newBuilder();
    db.setType(Message.MessageType.PUBLISHER_REQUEST);
    db.setPublishRequest(prb);
    //windowStartTime is ignored for now - shouldn't we?
    db.setWindowId((int)startingWindowId);

    final ChannelFutureListener cfl = new ChannelFutureListener()
    {
      public void operationComplete(ChannelFuture cf) throws Exception
      {
        Buffer.Payload.Builder pdb = Buffer.Payload.newBuilder();
        pdb.setData(ByteString.EMPTY);

        pdb.setPartition(new Random().nextInt());


        Buffer.Message.Builder db = Message.newBuilder();
        db.setType(Message.MessageType.PAYLOAD);
        db.setWindowId((int)new Date().getTime());
        db.setPayload(pdb);

        Thread.sleep(500);
        cf.channel().write(db).addListener(this);
      }

    };

    channel.write(db.build());//.addListener(cfl);
  }

  /**
   *
   * @param channel
   * @param id
   * @param down_type
   * @param node
   * @param mask
   * @param partitions
   * @param startingWindowId
   */
  public static void subscribe(
          final Channel channel,
          String id,
          String down_type,
          String node,
          int mask,
          Collection<Integer> partitions,
          long startingWindowId)
  {
    Buffer.SubscriberRequest.Builder srb = Buffer.SubscriberRequest.newBuilder();
    srb.setIdentifier(id);
    srb.setType(down_type);
    srb.setUpstreamIdentifier(node);
    srb.setBaseSeconds((int)(startingWindowId >> 32));

    if (partitions != null) {
      Buffer.Partitions.Builder bpb = Buffer.Partitions.newBuilder();
      bpb.setMask(mask);
      for (Integer c: partitions) {
        bpb.addPartition(c);
      }

      srb.setPartitions(bpb);
    }
    srb.setPolicy(Buffer.SubscriberRequest.PolicyType.ROUND_ROBIN);

    Message.Builder builder = Message.newBuilder();
    builder.setType(Message.MessageType.SUBSCRIBER_REQUEST);
    builder.setSubscribeRequest(srb);
    builder.setWindowId((int)startingWindowId);

    channel.write(builder.build()).addListener(new ChannelFutureListener()
    {
      public void operationComplete(ChannelFuture future) throws Exception
      {
//        /* subscriber never writes to the channel after initial request */
//        if (channel instanceof SocketChannel) {
//          ((SocketChannel)channel).shutdownOutput();
//        }
      }

    });
  }

  public static void purge(Channel channel, String id, long windowId)
  {
    Buffer.PurgeRequest.Builder prb = Buffer.PurgeRequest.newBuilder();
    prb.setBaseSeconds((int)(windowId >> 32));
    prb.setIdentifier(id);

    Message.Builder builder = Message.newBuilder();
    builder.setType(MessageType.PURGE_REQUEST);
    builder.setPurgeRequest(prb);
    builder.setWindowId((int)windowId);

    channel.write(builder.build());
  }

  public static void reset(Channel channel, String id, long windowId)
  {
    Buffer.ResetRequest.Builder rrb = Buffer.ResetRequest.newBuilder();
    rrb.setIdentifier(id);

    Message.Builder builder = Message.newBuilder();
    builder.setType(MessageType.RESET_REQUEST);
    builder.setResetRequest(rrb);
    builder.setWindowId((int)windowId);

    channel.write(builder.build());
  }

  /**
   *
   * @param arg0
   * @param arg1
   * @throws Exception
   */
  @Override
  public void messageReceived(ChannelHandlerContext arg0, Object arg1) throws Exception
  {
    logger.info("received message {}", arg1);
  }

  /**
   *
   * @param ctx
   * @param cause
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
  {
    logger.info("Unexpected exception {}", cause.getCause());

    try {
      ctx.channel().close();
    }
    catch (Exception e) {
    }
  }

}
