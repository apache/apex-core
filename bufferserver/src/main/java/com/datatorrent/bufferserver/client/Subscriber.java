/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.client;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.SubscribeRequestTuple;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 *
 * @since 0.3.2
 */
public abstract class Subscriber extends AbstractLengthPrependerClient
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
