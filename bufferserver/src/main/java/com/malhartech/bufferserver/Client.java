/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.netty.ClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Sends a list of continent/city pairs to a {@link LocalTimeServer} to get the
 * local times of the specified cities.
 */
public class Client
{
    private final String host;
    private final int port;
    private final String node;
    private final String type;
    private final String id;
    private final String down_type;
    private final Collection<byte[]> partitions;

    public Client(String host, int port, String node, String type, String id, String down_type, Collection<byte[]> partitions)
    {
        this.host = host;
        this.port = port;
        this.node = node;
        this.type = type;
        this.id = id;
        this.down_type = down_type;
        this.partitions = new ArrayList<byte[]>();
        this.partitions.addAll(partitions);
    }

    // publisher
    private Client(String host, int port, String node, String type)
    {
        this.host = host;
        this.port = port;
        this.node = node;
        this.type = type;
        this.id = null;
        this.down_type = null;
        this.partitions = null;
    }

    public void run() throws Exception
    {
        // Set up.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup())
                .channel(new NioSocketChannel())
                .remoteAddress(host, port)
                .handler(new ClientInitializer(ClientHandler.class));

        // Make a new connection.
        Channel channel = bootstrap.connect().sync().channel();

        if (id == null) {
            ClientHandler.publish(channel, node, type, 0L);
        }
        else {
            ClientHandler.registerPartitions(channel, id, down_type, node, type, partitions, 0L);
        }
    }

    public static void main(String[] args) throws Exception
    {
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
        }
        else { // downstream node
            String identifier = args[4];
            String down_type = args[5];
            Collection<byte[]> partitions = parsePartitions(args, 6);
            new Client(host, port, node, type, identifier, down_type, partitions).run();
        }
    }

    private static void printUsage()
    {
        System.err.println(
                "Usage: " + Client.class.getSimpleName()
                + " <host> <port> upstream_node_id upstream_node_type [downstream_node_id downstream_node_type [partitions ...]]");
        System.err.println(
                "Upstream Example: " + Client.class.getSimpleName()
                + " localhost 8080 map1 mapper");
        System.err.println(
                "Downstream Example: " + Client.class.getSimpleName()
                + " localhost 8080 map1 mapper reduce1 reduce 1 5 7");
    }

    private static List<byte[]> parsePartitions(String[] args, int offset)
    {
        List<byte[]> partitions = new ArrayList<byte[]>();
        for (int i = offset; i < args.length; i++) {
            partitions.add(args[i].trim().getBytes());
        }
        return partitions;
    }
}
