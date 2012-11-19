/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.PurgeRequest;
import com.malhartech.bufferserver.Buffer.ResetRequest;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.bufferserver.Buffer.SubscriberRequest;
import com.malhartech.bufferserver.policy.*;
import com.malhartech.bufferserver.util.SerializedData;
import io.netty.buffer.MessageBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.*;
import io.netty.util.AttributeKey;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler to serve connections accepted by the server<p>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Sharable
public class ServerHandler extends ChannelInboundHandlerAdapter implements ChannelInboundMessageHandler<Data>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ServerHandler.class);
  private static final AttributeKey<DataList> DATA_LIST = new AttributeKey<DataList>("ServerHandler.datalist");
  private static final AttributeKey<LogicalNode> LOGICAL_NODE = new AttributeKey<LogicalNode>("ServerHandler.logicalnode");
  private final HashMap<String, DataList> publisherBufffers = new HashMap<String, DataList>();
  private final HashMap<String, LogicalNode> subscriberGroups = new HashMap<String, LogicalNode>();
  private final ConcurrentHashMap<String, Channel> publisherChannels = new ConcurrentHashMap<String, Channel>();
  private final ConcurrentHashMap<String, Channel> subscriberChannels = new ConcurrentHashMap<String, Channel>();
  private final int bufferSize;

  public ServerHandler(int buffersize)
  {
    this.bufferSize = buffersize;
  }

  @Override
  public final void inboundBufferUpdated(ChannelHandlerContext ctx) throws Exception
  {
    DataList dl = ctx.attr(DATA_LIST).get();

    MessageBuf<Data> in = ctx.inboundMessageBuffer();
//    logger.debug("InboundBuffer updated with {} messages", in.size());
    for (int i = in.size(); i-- > 0;) {
      Data data = in.poll();
      switch (data.getType()) {
        case PUBLISHER_REQUEST:
          logger.info("Received publisher request: {}", data);
          dl = handlePublisherRequest(data.getPublishRequest(), ctx);
          dl.rewind(data.getPublishRequest().getBaseSeconds(), data.getWindowId(), new ProtobufDataInspector());
          ctx.attr(DATA_LIST).set(dl);
          break;

        case SUBSCRIBER_REQUEST:
          logger.info("Received subscriber request: {}", data);
          boolean contains = subscriberGroups.containsKey(data.getSubscribeRequest().getType());
          LogicalNode ln = handleSubscriberRequest(data.getSubscribeRequest(), ctx, data.getWindowId());
          if (!contains) {
            ln.catchUp();
          }
          ctx.attr(LOGICAL_NODE).set(ln);
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", data);
          handlePurgeRequest(data.getPurgeRequest(), ctx, data.getWindowId());
          break;

        case RESET_REQUEST:
          logger.info("Received purge all request: {}", data);
          handleResetRequest(data.getResetRequest(), ctx, data.getWindowId());
          break;

        default:
          if (dl == null) {
            logger.error("Received packet {} when there is no datalist defined", data);
          }
          else {
            dl.add(data);
          }
          break;
      }
    }

    if (dl != null) {
      dl.flush();
    }
  }

  /**
   *
   * @param request
   * @param ctx
   * @param windowId
   */
  public synchronized DataList handlePublisherRequest(Buffer.PublisherRequest request, ChannelHandlerContext ctx)
  {
    /* we are never going to write to the publisher socket */
//    if (ctx.channel() instanceof SocketChannel) {
//      ((SocketChannel)ctx.channel()).shutdownOutput().addListener(new ChannelFutureListener() {
//
//        public void operationComplete(ChannelFuture future) throws Exception
//        {
//          logger.debug("future = {}", future.isSuccess());
//        }
//      });
//    }

    String identifier = request.getIdentifier();

    DataList dl;

    if (publisherBufffers.containsKey(identifier)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      Channel previous = publisherChannels.put(identifier, ctx.channel());
      if (previous != null && previous.id() != ctx.channel().id()) {
        previous.close();
      }

      dl = publisherBufffers.get(identifier);
    }
    else {
      dl = new DataList(identifier, bufferSize);
      publisherBufffers.put(identifier, dl);
    }

    return dl;
  }

  /**
   *
   * @param request
   * @param ctx
   * @param windowId
   */
  public synchronized LogicalNode handleSubscriberRequest(SubscriberRequest request, ChannelHandlerContext ctx, int windowId)
  {
    String identifier = request.getIdentifier();
    String type = request.getType();
    String upstream_identifier = request.getUpstreamIdentifier();
    //String upstream_type = request.getUpstreamType();

    // Check if there is a logical node of this type, if not create it.
    LogicalNode ln;
    if (subscriberGroups.containsKey(type)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */
      Channel previous = subscriberChannels.put(identifier, ctx.channel());
      if (previous != null && previous.id() != ctx.channel().id()) {
        previous.close();
      }

      ln = subscriberGroups.get(type);
      ln.addChannel(ctx.channel());
    }
    else {
      /*
       * if there is already a datalist registered for the type in which this client is interested,
       * then get a iterator on the data items of that data list. If the datalist is not registered,
       * then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      DataList dl;
      if (publisherBufffers.containsKey(upstream_identifier)) {
        dl = publisherBufffers.get(upstream_identifier);
      }
      else {
        dl = new DataList(upstream_identifier, bufferSize);
        publisherBufffers.put(upstream_identifier, dl);
      }

      ln = new LogicalNode(upstream_identifier,
                           type,
                           dl.newIterator(identifier, new ProtobufDataInspector(), windowId),
                           getPolicy(request.getPolicy(), null),
                           (long)request.getBaseSeconds() << 32 | windowId);

      if (request.getPartitionCount() > 0) {
        for (ByteString bs: request.getPartitionList()) {
          ln.addPartition(bs.asReadOnlyByteBuffer());
        }
      }

      subscriberGroups.put(type, ln);
      ln.addChannel(ctx.channel());
      dl.addDataListener(ln);
    }

    return ln;
  }

  /**
   *
   * @param policytype
   * @param type
   * @return Policy
   */
  public Policy getPolicy(Buffer.SubscriberRequest.PolicyType policytype, String type)
  {
    Policy p = null;

    switch (policytype) {
      case CUSTOM:
        try {
          Class<?> customclass = Class.forName(type);
          p = (Policy)customclass.newInstance();
        }
        catch (InstantiationException ex) {
          Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (IllegalAccessException ex) {
          Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch (ClassNotFoundException ex) {
          Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        break;

      case GIVE_ALL:
        p = GiveAll.getInstance();
        break;

      case LEAST_BUSY:
        p = LeastBusy.getInstance();
        break;

      case RANDOM_ONE:
        p = RandomOne.getInstance();
        break;

      case ROUND_ROBIN:
        p = new RoundRobin();
        break;
    }

    return p;
  }

  @Override
  public synchronized void channelInactive(ChannelHandlerContext ctx) throws Exception
  {
    Channel c = ctx.channel();

    DataList dl = ctx.attr(DATA_LIST).get();
    if (dl != null) {
      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless a new publisher comes up with the same name. We leave
       * it to the stream to decide when to bring up a new node with the same identifier as the one which just died.
       */
      if (publisherChannels.containsValue(c)) {
        final Iterator<Entry<String, Channel>> i = publisherChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == c) {
            i.remove();
            break;
          }
        }
      }
      ctx.attr(DATA_LIST).remove();
    }

    LogicalNode ln = ctx.attr(LOGICAL_NODE).get();
    if (ln != null) {
      if (subscriberChannels.containsValue(c)) {
        final Iterator<Entry<String, Channel>> i = subscriberChannels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == c) {
            i.remove();
            break;
          }
        }
      }

      ln.removeChannel(c);
      if (ln.getPhysicalNodeCount() == 0) {
        dl = publisherBufffers.get(ln.getUpstream());
        if (dl != null) {
          dl.removeDataListener(ln);
          dl.delIterator(ln.getIterator());
        }
        subscriberGroups.remove(ln.getGroup());
      }

      ctx.attr(LOGICAL_NODE).remove();
    }
  }

  /**
   *
   * @param ctx
   * @param cause
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
  {
    if (cause instanceof java.nio.channels.ClosedChannelException) {
    }
    else if (cause instanceof java.io.IOException) {
    }
    else {
      logger.info("unexpected exception", cause);
    }

    try {
      channelInactive(ctx);
      ctx.channel().close();
    }
    catch (Exception e) {
    }
  }

  private synchronized void handlePurgeRequest(PurgeRequest request, ChannelHandlerContext ctx, int windowId)
  {
    DataList dl;
    dl = publisherBufffers.get(request.getIdentifier());

    SimpleData.Builder sdb = SimpleData.newBuilder();
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      dl.purge(request.getBaseSeconds(), windowId, new ProtobufDataInspector());
      sdb.setData(ByteString.copyFromUtf8("Purge request sent for processing"));
    }

    Data.Builder db = Data.newBuilder();
    db.setType(DataType.SIMPLE_DATA);
    db.setWindowId(windowId);
    db.setSimpleData(sdb);

    ctx.write(SerializedData.getInstanceFrom(db.build()))
            .addListener(ChannelFutureListener.CLOSE);
  }

  public MessageBuf<Data> newInboundBuffer(ChannelHandlerContext ctx) throws Exception
  {
    return Unpooled.messageBuffer();
  }

  private synchronized void handleResetRequest(ResetRequest request, ChannelHandlerContext ctx, int windowId)
  {
    DataList dl;
    dl = publisherBufffers.remove(request.getIdentifier());

    SimpleData.Builder sdb = SimpleData.newBuilder();
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      Channel channel = publisherChannels.remove(request.getIdentifier());
      if (channel != null) {
        channel.flush().awaitUninterruptibly();
        channel.close();
      }
      dl.reset();
      sdb.setData(ByteString.copyFromUtf8("Reset request sent for processing"));
    }

    Data.Builder db = Data.newBuilder();
    db.setType(DataType.SIMPLE_DATA);
    db.setWindowId(windowId);
    db.setSimpleData(sdb);

    ctx.write(SerializedData.getInstanceFrom(db.build()))
            .addListener(ChannelFutureListener.CLOSE);
  }
}
