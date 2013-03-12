/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends a list of continent/city pairs to a LocalTimeServer to get the local times of the specified cities<p>
 * <br>
 * Is an application
 */
public class ClientTest
{
  private final String host;
  private final int port;
  private final String node;
  private final String type;
  private final String id;
  private final String down_type;
  private final int mask;
  private final Collection<Integer> partitions;
  private final long windowId;
  private final boolean purge;

  /**
   *
   * @param host
   * @param port
   * @param node
   * @param type
   * @param id
   * @param down_type
   * @param mask
   * @param partitions
   */
  public ClientTest(String host, int port, String node, String type, String id, String down_type, int mask, Collection<Integer> partitions)
  {
    this.host = host;
    this.port = port;
    this.node = node;
    this.type = type;
    this.id = id;
    this.down_type = down_type;
    this.mask = mask;
    this.partitions = new ArrayList<Integer>();
    this.partitions.addAll(partitions);
    windowId = 0;
    purge = false;
  }

  // publisher
  private ClientTest(String host, int port, String node, String type)
  {
    this.host = host;
    this.port = port;
    this.node = node;
    this.type = type;
    id = null;
    down_type = null;
    this.mask = 0;
    partitions = null;
    windowId = 0;
    purge = false;
  }

  private ClientTest(String host, int port, String publisher_id, long lastWindowId)
  {
    this.mask = 0;
    partitions = null;
    down_type = null;
    type = null;
    node = null;
    this.host = host;
    this.port = port;
    this.id = publisher_id;
    this.windowId = lastWindowId;
    purge = true;
  }

  /**
   *
   * @throws Exception
   */
  public void run() throws Exception
  {
//    // Set up.
//    Bootstrap bootstrap = new Bootstrap();
//    bootstrap.group(new NioEventLoopGroup(1, new NameableThreadFactory("BSClient")))
//            .channel(NioSocketChannel.class)
//            //            .option(ChannelOption.ALLOW_HALF_CLOSURE, true)
//            .remoteAddress(host, port)
//            .handler(new ClientInitializer(new ClientHandler()));
//
//    // Make a new connection.
//    Channel channel = bootstrap.connect().sync().channel();
//
//    if (purge) {
//      ClientHandler.purge(channel, id, windowId);
//    }
//    else if (id == null) {
//      ClientHandler.publish(channel, node, type, 0L);
//    }
//    else {
//      ClientHandler.subscribe(channel, id, down_type, node, mask, partitions, 0L);
//    }
  }

  /**
   *
   * @param args
   * @throws Exception
   */
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
      new ClientTest(host, port, node, type).run();
    }
    else { // downstream node
      String identifier = args[4];
      String down_type = args[5];
      int mask = Integer.parseInt(args[6]);
      Collection<Integer> partitions = parsePartitions(args, 7);
      new ClientTest(host, port, node, type, identifier, down_type, mask, partitions).run();
    }
  }

  private static void printUsage()
  {
    logger.info(
            "Usage: " + ClientTest.class.getSimpleName()
            + " <host> <port> upstream_node_id upstream_node_type [downstream_node_id downstream_node_type [partitions ...]]");
    logger.info(
            "Upstream Example: " + ClientTest.class.getSimpleName()
            + " localhost 8080 map1 mapper");
    logger.info(
            "Downstream Example: " + ClientTest.class.getSimpleName()
            + " localhost 8080 map1 mapper reduce1 reduce 1 5 7");
  }

  private static List<Integer> parsePartitions(String[] args, int offset)
  {
    List<Integer> partitions = new ArrayList<Integer>();
    for (int i = offset; i < args.length; i++) {
      partitions.add(Integer.parseInt(args[i].trim()));
    }
    return partitions;
  }

  private static final Logger logger = LoggerFactory.getLogger(ClientTest.class);
}
