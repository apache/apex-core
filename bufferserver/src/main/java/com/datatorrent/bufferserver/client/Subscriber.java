/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.SubscribeRequestTuple;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 */
public abstract class Subscriber extends AbstractClient
{
  private final String id;

  public Subscriber(String id)
  {
    super(64 * 1024, 1024);
    this.id = id;
  }

  public void activate(String version, String type, String sourceId, int mask, Collection<Integer> partitions, long windowId)
  {
    write(SubscribeRequestTuple.getSerializedRequest(
            version,
            id,
            type,
            sourceId,
            mask,
            partitions,
            windowId));
  }

  @Override
  public String toString()
  {
    return "Subscriber{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);
}
