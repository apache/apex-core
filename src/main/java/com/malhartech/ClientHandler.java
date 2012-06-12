/*
 * Copyright 2011 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.malhartech;

import com.google.protobuf.ByteString;
import com.malhartech.Buffer.Data;
import com.malhartech.Buffer.SubscriberRequest;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jboss.netty.channel.*;

/**
 * this class is called the last while reading the response from server.
 *
 * @author chetan
 */
public class ClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(
            ClientHandler.class.getName());
    // Stateful properties
    private volatile Channel channel;

    public void registerPartitions(String node, Collection<String> partitions) {
        SubscriberRequest.Builder srb = SubscriberRequest.newBuilder();
        srb.setNode(node);

        for (String c : partitions) {
            srb.addPartition(ByteString.copyFromUtf8(c));
        }

        Data.Builder builder = Data.newBuilder();
        builder.setType(Data.DataType.SUBSCRIBER_REQUEST);
        builder.setSubscribe(srb);

        channel.write(builder.build());
    }

    @Override
    public void handleUpstream(
            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            logger.info(e.toString());
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        channel = e.getChannel();
        super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, final MessageEvent e) {
        Data data = (Data) e.getMessage();
        System.out.println(data.getType());
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
}
