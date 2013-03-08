/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.Buffer.PublisherRequest;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is called the last while reading the response from server<p>
 * <br>
 *
 * @author chetan
 */
public class ClientHandler
{
  private static final Logger logger = LoggerFactory.getLogger(ClientHandler.class);

  /**
   *
   * @param identifier
   * @param type
   * @param startingWindowId
   * @return
   */
  public static byte[] getPublishRequest(String identifier, String type, long startingWindowId)
  {
    Buffer.Request.Builder request = Buffer.Request.newBuilder();
    request.setIdentifier(identifier).setBaseSeconds((int)(startingWindowId >> 32)).setWindowId((int)startingWindowId);
    request.setExtension(PublisherRequest.request, Buffer.PublisherRequest.getDefaultInstance());
    Message.Builder db = Message.newBuilder();
    db.setType(Message.MessageType.PUBLISHER_REQUEST);
    db.setRequest(request);
    return db.build().toByteArray();
  }

  /**
   *
   * @param id
   * @param down_type
   * @param node
   * @param mask
   * @param partitions
   * @param startingWindowId
   * @return
   */
  public static byte[] getSubscribeRequest(
          String id,
          String down_type,
          String node,
          int mask,
          Collection<Integer> partitions,
          long startingWindowId)
  {
    Buffer.Request.Builder request = Buffer.Request.newBuilder();
    request.setIdentifier(id);
    request.setBaseSeconds((int)(startingWindowId >> 32));
    request.setWindowId((int)startingWindowId);

    Buffer.SubscriberRequest.Builder srb = Buffer.SubscriberRequest.newBuilder();
    srb.setType(down_type);
    srb.setUpstreamIdentifier(node);

    if (partitions != null) {
      Buffer.SubscriberRequest.Partitions.Builder bpb = Buffer.SubscriberRequest.Partitions.newBuilder();
      bpb.setMask(mask);
      for (Integer c : partitions) {
        bpb.addPartition(c);
      }

      srb.setPartitions(bpb);
    }
    srb.setPolicy(Buffer.SubscriberRequest.PolicyType.ROUND_ROBIN);

    request.setExtension(Buffer.SubscriberRequest.request, srb.build());
    Message.Builder builder = Message.newBuilder();
    builder.setType(Message.MessageType.SUBSCRIBER_REQUEST);
    builder.setRequest(request);

    return builder.build().toByteArray();
  }

  public static byte[] getPurgeRequest(String id, long windowId)
  {
    Buffer.Request.Builder request = Buffer.Request.newBuilder();
    request.setBaseSeconds((int)(windowId >> 32));
    request.setWindowId((int)windowId);
    request.setIdentifier(id);

    request.setExtension(Buffer.PurgeRequest.request, Buffer.PurgeRequest.getDefaultInstance());

    Message.Builder builder = Message.newBuilder();
    builder.setType(MessageType.PURGE_REQUEST);
    builder.setRequest(request);

    return builder.build().toByteArray();
  }

  public static byte[] getResetRequest(String id, long windowId)
  {
    Buffer.Request.Builder request = Buffer.Request.newBuilder();
    request.setIdentifier(id);
    request.setExtension(Buffer.ResetRequest.request, Buffer.ResetRequest.getDefaultInstance());

    Message.Builder builder = Message.newBuilder();
    builder.setType(MessageType.RESET_REQUEST);
    builder.setRequest(request);

    return builder.build().toByteArray();
  }
}
