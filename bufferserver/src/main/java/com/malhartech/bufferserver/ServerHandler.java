package com.malhartech.bufferserver;
/*
 * Copyright 2011 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import com.malhartech.bufferserver.policy.LeastBusy;
import com.malhartech.bufferserver.policy.Policy;
import com.malhartech.bufferserver.policy.RandomOne;
import com.malhartech.bufferserver.policy.GiveAll;
import com.malhartech.bufferserver.policy.RoundRobin;
import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.channel.*;

public class ServerHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(ServerHandler.class.getName());
    final HashMap<String, DataList> publisher_bufffers = new HashMap<String, DataList>();
    final HashMap<String, LogicalNode> groups = new HashMap<String, LogicalNode>();

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {

        final Data data = (Data) e.getMessage();

        switch (data.getType()) {
            case PUBLISHER_REQUEST:
                handlePublisherRequest(data.getPublish(), ctx);
                break;


            case SUBSCRIBER_REQUEST:
                handleSubscriberRequest(data.getSubscribe(), ctx);
                break;

            case BEGIN_WINDOW:
            case END_WINDOW:
            case HEARTBEAT_DATA:
            case SIMPLE_DATA:
            case PARTITIONED_DATA:
            case SERDE_CODE:
                ((DataList) ctx.getAttachment()).add(data);
                break;
        }
    }

    public void handlePublisherRequest(Buffer.PublisherRequest request, ChannelHandlerContext ctx) {
        String identifier = request.getIdentifier();
        String type = request.getType();

        DataList dl;

        synchronized (publisher_bufffers) {
            if (publisher_bufffers.containsKey(identifier)) {
                dl = (DataList) publisher_bufffers.get(identifier);
                // if this is the case some readers might be waiting already
                // activate them
            } else {
                dl = new DataList(identifier, type, 1024 * 1024);
                publisher_bufffers.put(identifier, dl);
            }
        }

        ctx.setAttachment(dl);
    }

    public void handleSubscriberRequest(Buffer.SubscriberRequest request, ChannelHandlerContext ctx) {
        String identifier = request.getIdentifier();
        String type = request.getType();
        String upstream_identifier = request.getUpstreamIdentifier();
        String upstream_type = request.getUpstreamType();

        // Check if there is a logical node of this type, if not create it.
        LogicalNode ln;
        if (groups.containsKey(type)) {
            ln = groups.get(type);
            ln.addChannel(ctx.getChannel());
        } else {
            /**
             * if there is already a datalist registered for the type in which
             * this client is interested, then get a iterator on the data items
             * of that data list. If the datalist is not registered, then create
             * one and register it. Hopefully this one would be used by future
             * upstream nodes.
             */
            DataList dl;
            synchronized (publisher_bufffers) {
                if (publisher_bufffers.containsKey(upstream_identifier)) {
                    dl = (DataList) publisher_bufffers.get(upstream_identifier);
                } else {
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
//            if (request.hasStartingWindowid()) {
//                ln.catchUp(request.getStartingWindowid());
//            }
        }

        ctx.setAttachment(ln);
    }

    public Policy getPolicy(Buffer.SubscriberRequest.PolicyType policytype, String type) {
        Policy p = null;

        switch (policytype) {
            case CUSTOM:
                try {
                    Class<?> customclass = Class.forName(type);
                    p = (Policy) customclass.newInstance();
                } catch (InstantiationException ex) {
                    Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(ServerHandler.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ClassNotFoundException ex) {
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
            ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        e.getChannel().close();
    }

    /*
     * @Override public void writeRequested(ChannelHandlerContext ctx,
     * MessageEvent e) throws Exception { // when the control comes here, we are
     * in a worker thread which is supposed // to be writing. We have only
     * written one unit of data on the socket, // take this opportunity to
     * continue to write lots of data, we do not care // if this is a worker
     * thread that gets blocked. or should we worry?
     * System.err.println("writeRequested from thread " +
     * Thread.currentThread()); super.writeRequested(ctx, e); }
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (ctx.getAttachment() instanceof DataList) {
            /**
             * since the publisher server died, the queue which it was using
             * would stop pumping the data unless a new publisher comes up with
             * the same name. We leave it to the stream to decide when to bring
             * up a new node with the same identifier as the one which just
             * died.
             */
            ((DataList) ctx.getAttachment()).addPublisherDisconnected(System.currentTimeMillis());
            ctx.setAttachment(null);
        }
    }
}
