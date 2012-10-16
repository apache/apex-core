/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.PurgeRequest;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.bufferserver.Buffer.SubscriberRequest;
import com.malhartech.bufferserver.policy.*;
import com.malhartech.bufferserver.util.SerializedData;
import io.netty.buffer.MessageBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.ChannelHandler.Sharable;
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
  private static final AttributeKey<DataList> DATALIST = new AttributeKey<DataList>("ServerHandler.datalist");
  private static final AttributeKey<LogicalNode> LOGICALNODE = new AttributeKey<LogicalNode>("ServerHandler.logicalnode");
  final HashMap<String, DataList> publisher_bufffers = new HashMap<String, DataList>();
  final HashMap<String, LogicalNode> groups = new HashMap<String, LogicalNode>();
  final ConcurrentHashMap<String, Channel> publisher_channels = new ConcurrentHashMap<String, Channel>();
  final ConcurrentHashMap<String, Channel> subscriber_channels = new ConcurrentHashMap<String, Channel>();

  @Override
  public final void inboundBufferUpdated(ChannelHandlerContext ctx) throws Exception
  {
    DataList dl = ctx.attr(DATALIST).get();

    MessageBuf<Data> in = ctx.inboundMessageBuffer();
    logger.debug("InboundBuffer updated with {} messages", in.size());
    for (int i = in.size(); i-- > 0;) {
      Data data = in.poll();
      switch (data.getType()) {
        case PUBLISHER_REQUEST:
          logger.info("Received publisher request: {}", data);
          dl = handlePublisherRequest(data.getPublishRequest(), ctx, data.getWindowId());
          break;

        case SUBSCRIBER_REQUEST:
          logger.info("Received subscriber request: {}", data);
          handleSubscriberRequest(data.getSubscribeRequest(), ctx, data.getWindowId());
          break;

        case PURGE_REQUEST:
          logger.info("Received purge request: {}", data);
          handlePurgeRequest(data.getPurgeRequest(), ctx, data.getWindowId());
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
  public DataList handlePublisherRequest(Buffer.PublisherRequest request, ChannelHandlerContext ctx, int windowId)
  {
    String identifier = request.getIdentifier();
    String type = request.getType();

    DataList dl;

    synchronized (publisher_bufffers) {
      if (publisher_bufffers.containsKey(identifier)) {
        /*
         * close previous connection with the same identifier which is guaranteed to be unique.
         */
        Channel previous = publisher_channels.put(identifier, ctx.channel());
        if (previous != null && previous.id() != ctx.channel().id()) {
          previous.close();
        }

        dl = publisher_bufffers.get(identifier);
      }
      else {
        dl = new DataList(identifier, type);
        publisher_bufffers.put(identifier, dl);
      }
    }

    dl.rewind(request.getBaseSeconds(), windowId, new ProtobufDataInspector());
    ctx.attr(DATALIST).set(dl);
    return dl;
  }

  /**
   *
   * @param request
   * @param ctx
   * @param windowId
   */
  public LogicalNode handleSubscriberRequest(SubscriberRequest request, ChannelHandlerContext ctx, int windowId)
  {
    String identifier = request.getIdentifier();
    String type = request.getType();
    String upstream_identifier = request.getUpstreamIdentifier();
    //String upstream_type = request.getUpstreamType();

    // Check if there is a logical node of this type, if not create it.
    LogicalNode ln;
    if (groups.containsKey(type)) {
      /*
       * close previous connection with the same identifier which is guaranteed to be unique.
       */

      Channel previous = subscriber_channels.put(identifier, ctx.channel());
      if (previous != null && previous.id() != ctx.channel().id()) {
        previous.close();
      }

      ln = groups.get(type);
      ln.addChannel(ctx.channel());
    }
    else {
      /**
       * if there is already a datalist registered for the type in which this client is interested, then get a iterator on the data items of that data list. If
       * the datalist is not registered, then create one and register it. Hopefully this one would be used by future upstream nodes.
       */
      DataList dl;
      synchronized (publisher_bufffers) {
        if (publisher_bufffers.containsKey(upstream_identifier)) {
          dl = publisher_bufffers.get(upstream_identifier);
        }
        else {
          dl = new DataList(upstream_identifier, type);
          publisher_bufffers.put(upstream_identifier, dl);
        }
      }

      ln = new LogicalNode(upstream_identifier,
                           type,
                           dl.newIterator(identifier, new ProtobufDataInspector()),
                           getPolicy(request.getPolicy(), null));

      if (request.getPartitionCount() > 0) {
        for (ByteString bs: request.getPartitionList()) {
          ln.addPartition(bs.asReadOnlyByteBuffer());
        }
      }

      groups.put(type, ln);
      ln.addChannel(ctx.channel());
      dl.addDataListener(ln);
      ln.catchUp(((long)request.getBaseSeconds() << 32) | windowId);
    }

    ctx.attr(LOGICALNODE).set(ln);
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
  public void channelInactive(ChannelHandlerContext ctx) throws Exception
  {
    Channel c = ctx.channel();

    DataList dl = ctx.attr(DATALIST).get();
    if (dl != null) {
      /**
       * since the publisher server died, the queue which it was using would stop pumping the data unless a new publisher comes up with the same name. We leave
       * it to the stream to decide when to bring up a new node with the same identifier as the one which just died.
       */
      if (publisher_channels.containsValue(c)) {
        final Iterator<Entry<String, Channel>> i = publisher_channels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == c) {
            i.remove();
            break;
          }
        }
      }
      ctx.attr(DATALIST).remove();
    }

    LogicalNode ln = ctx.attr(LOGICALNODE).get();
    if (ln != null) {
      if (subscriber_channels.containsValue(c)) {
        final Iterator<Entry<String, Channel>> i = subscriber_channels.entrySet().iterator();
        while (i.hasNext()) {
          if (i.next().getValue() == c) {
            i.remove();
            break;
          }
        }
      }

      ln.removeChannel(c);
      if (ln.getPhysicalNodeCount() == 0) {
        dl = publisher_bufffers.get(ln.getUpstream());
        if (dl != null) {
          dl.removeDataListener(ln);
          dl.delIterator(ln.getIterator());
        }
        groups.remove(ln.getGroup());
      }

      ctx.attr(LOGICALNODE).remove();
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
    if (!(cause instanceof java.nio.channels.ClosedChannelException)) {
    logger.info("Error on ctx = {} because of {}", ctx, cause);
    }

    try {
      channelInactive(ctx);
      ctx.channel().close();
    }
    catch (Exception e) {
    }
  }

  private void handlePurgeRequest(PurgeRequest request, ChannelHandlerContext ctx, int windowId)
  {
    DataList dl;
    synchronized (publisher_bufffers) {
      dl = publisher_bufffers.get(request.getIdentifier());
    }

    SimpleData.Builder sdb = SimpleData.newBuilder();
    if (dl == null) {
      sdb.setData(ByteString.copyFromUtf8("Invalid identifier '" + request.getIdentifier() + "'"));
    }
    else {
      dl.purge(request.getBaseSeconds(), windowId, new ProtobufDataInspector());
      sdb.setData(ByteString.copyFromUtf8("Request sent for processing"));
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
}
