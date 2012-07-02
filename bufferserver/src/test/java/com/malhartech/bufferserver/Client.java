/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.netty.ClientPipelineFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * Sends a list of continent/city pairs to a {@link LocalTimeServer} to get the
 * local times of the specified cities.
 */
public class Client {

    private final String host;
    private final int port;
    private final String node;
    private final String type;
    private final String id;
    private final String down_type;
    private final Collection<String> partitions;

    public Client(String host, int port, String node, String type, String id, String down_type, Collection<String> partitions) {
        this.host = host;
        this.port = port;
        this.node = node;
        this.type = type;
        this.id = id;
        this.down_type = down_type;
        this.partitions = new ArrayList<String>();
        this.partitions.addAll(partitions);
    }

    private Client(String host, int port, String node, String type) {
        this.host = host;
        this.port = port;
        this.node = node;
        this.type = type;
        this.id = null;
        this.down_type = null;
        this.partitions = null;
    }

    public void run() {
        // Set up.
        ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

        // Configure the event pipeline factory.
        bootstrap.setPipelineFactory(new ClientPipelineFactory(ClientHandler.class));

        // Make a new connection.
        ChannelFuture connectFuture =
                bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection is made successfully.
        Channel channel = connectFuture.awaitUninterruptibly().getChannel();

        // Get the handler instance to initiate the request.
        ClientHandler handler =
                channel.getPipeline().get(ClientHandler.class);

        if (id == null) {
            handler.publish(node, type);
        } else {
            handler.registerPartitions(id, down_type, node, type, partitions);
        }
    }

    

    public static void main(String[] args) throws Exception {
        // Print usage if necessary.
        if (args.length < 4) {
            printUsage();
            return;
        }

        // Parse options.
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        String node = args[2];
        String type = args[3];


        if (args.length == 4) { // upstream node
            new Client(host, port, node, type).run();
        } else { // downstream node
            String identifier = args[4];
            String down_type = args[5];
            Collection<String> partitions = parsePartitions(args, 6);
            new Client(host, port, node, type, identifier, down_type, partitions).run();
        }
    }

    private static void printUsage() {
        System.err.println(
                "Usage: " + Client.class.getSimpleName()
                + " <host> <port> upstream_node_id upstream_node_type [downstream_node_id [partitions ...]]");
        System.err.println(
                "Upstream Example: " + Client.class.getSimpleName()
                + " localhost 8080 map1 mapper");
        System.err.println(
                "Downstream Example: " + Client.class.getSimpleName()
                + " localhost 8080 map1 mapper reduce1 1 5 7");
    }

    private static List<String> parsePartitions(String[] args, int offset) {
        List<String> partitions = new ArrayList<String>();
        for (int i = offset; i < args.length; i++) {
            partitions.add(args[i].trim());
        }
        return partitions;
    }
}
