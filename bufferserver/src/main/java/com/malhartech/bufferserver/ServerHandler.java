/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.SubscriberRequest;
import com.malhartech.bufferserver.policy.*;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.channel.*;

/**
 * Handler to serve connections accepted by the server.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ServerHandler extends SimpleChannelUpstreamHandler
{

  private static final Logger logger = Logger.getLogger(ServerHandler.class.getName());
  final HashMap<String, DataList> publisher_bufffers = new HashMap<String, DataList>();
  final HashMap<String, LogicalNode> groups = new HashMap<String, LogicalNode>();

  @Override
  public void messageReceived(
          ChannelHandlerContext ctx, MessageEvent e)
  {

    logger.log(Level.INFO, "======== entering current thread = {0}", Thread.currentThread());
    final Data data = (Data) e.getMessage();

    switch (data.getType()) {
      case PUBLISHER_REQUEST:
        handlePublisherRequest(data.getPublish(), ctx, data.getWindowId());
        break;


      case SUBSCRIBER_REQUEST:
        handleSubscriberRequest(data.getSubscribe(), ctx, data.getWindowId());
        break;

      case BEGIN_WINDOW:
      case END_WINDOW:
      case HEARTBEAT_DATA:
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
      case SERDE_CODE:
        DataList dl = (DataList) ctx.getAttachment();
        if (dl == null) {
          logger.log(Level.INFO, "Attempt to send data w/o talking protocol");
        }
        else {
          logger.log(Level.INFO, "Sent {0} packet", data.getType());
          dl.add(data);
        }
        break;
    }
    logger.log(Level.INFO, "========== leaving current thread = {0}", Thread.currentThread());
  }

  public void handlePublisherRequest(Buffer.PublisherRequest request, ChannelHandlerContext ctx, long windowId)
  {
    String identifier = request.getIdentifier();
    String type = request.getType();
    logger.log(Level.INFO, "received publisher request: id={0}, type={1}", new Object[]{request.getIdentifier(), request.getType()});

    DataList dl;

    synchronized (publisher_bufffers) {
      if (publisher_bufffers.containsKey(identifier)) {
        dl = publisher_bufffers.get(identifier);
        // if this is the case some readers might be waiting already
        // activate them
      }
      else {
        dl = new DataList(identifier, type, 1024 * 1024);
        publisher_bufffers.put(identifier, dl);
      }
    }

    ctx.setAttachment(dl);
  }

  public void handleSubscriberRequest(SubscriberRequest request, ChannelHandlerContext ctx, long windowId)
  {
    String identifier = request.getIdentifier();
    String type = request.getType();
    String upstream_identifier = request.getUpstreamIdentifier();
    //String upstream_type = request.getUpstreamType();
    logger.log(Level.INFO, "received subscriber request: id={0}, type={1}, upstreamId={2}", new Object[]{request.getIdentifier(), request.getType(), request.getUpstreamIdentifier()});

    // Check if there is a logical node of this type, if not create it.
    LogicalNode ln;
    if (groups.containsKey(type)) {
      ln = groups.get(type);
      ln.addChannel(ctx.getChannel());
    }
    else {
      /**
       * if there is already a datalist registered for the type in which this
       * client is interested, then get a iterator on the data items of that
       * data list. If the datalist is not registered, then create one and
       * register it. Hopefully this one would be used by future upstream nodes.
       */
      DataList dl;
      synchronized (publisher_bufffers) {
        if (publisher_bufffers.containsKey(upstream_identifier)) {
          dl = publisher_bufffers.get(upstream_identifier);
        }
        else {
          dl = new DataList(upstream_identifier, type, 1024 * 1024);
          publisher_bufffers.put(upstream_identifier, dl);
        }
      }

      ln = new LogicalNode(type, dl.newIterator(identifier), getPolicy(request.getPolicy(), null));
      if (request.getPartitionCount() > 0) {
        for (ByteString bs : request.getPartitionList()) {
          ln.addPartition(bs.asReadOnlyByteBuffer());
        }
      }
      groups.put(type, ln);
      ln.addChannel(ctx.getChannel());
      dl.addDataListener(ln);
      ln.catchUp(windowId);
    }

    ctx.setAttachment(ln);
  }

  public Policy getPolicy(Buffer.SubscriberRequest.PolicyType policytype, String type)
  {
    Policy p = null;

    switch (policytype) {
      case CUSTOM:
        try {
          Class<?> customclass = Class.forName(type);
          p = (Policy) customclass.newInstance();
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
  public void exceptionCaught(
          ChannelHandlerContext ctx, ExceptionEvent e)
  {
    logger.log(
            Level.WARNING,
            "Unexpected exception from downstream.",
            e.getCause());
    e.getChannel().close();
  }

  /*
   * @Override public void writeRequested(ChannelHandlerContext ctx,
   * MessageEvent e) throws Exception { // when the control comes here, we are
   * in a worker thread which is supposed // to be writing. We have only written
   * one unit of data on the socket, // take this opportunity to continue to
   * write lots of data, we do not care // if this is a worker thread that gets
   * blocked. or should we worry? System.err.println("writeRequested from thread
   * " + Thread.currentThread()); super.writeRequested(ctx, e); }
   */
  @Override
  public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception
  {
    if (ctx.getAttachment() instanceof DataList) {
      /**
       * since the publisher server died, the queue which it was using would
       * stop pumping the data unless a new publisher comes up with the same
       * name. We leave it to the stream to decide when to bring up a new node
       * with the same identifier as the one which just died.
       */
      ((DataList) ctx.getAttachment()).addPublisherDisconnected(System.currentTimeMillis());
      ctx.setAttachment(null);
    }
  }
}
