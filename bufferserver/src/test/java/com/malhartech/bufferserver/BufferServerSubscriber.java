/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
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
public class BufferServerSubscriber extends AbstractSocketSubscriber<Buffer.Data>
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
  private final String sourceId;
  private final Collection<byte[]> partitions;
  AtomicInteger tupleCount = new AtomicInteger(0);
  Data firstPayload, lastPayload;
  long windowId;

  public BufferServerSubscriber(String sourceId, Collection<byte[]> partitions)
  {
    this.sourceId = sourceId;
    this.partitions = partitions;
  }

  @Override
  public void activate()
  {
    tupleCount.set(0);
    firstPayload = lastPayload = null;
    super.activate();
    ClientHandler.subscribe(channel,
                            "BufferServerSubscriber",
                            "BufferServerOutput/BufferServerSubscriber",
                            sourceId,
                            partitions,
                            windowId);
  }

  @Override
  public void messageReceived(io.netty.channel.ChannelHandlerContext ctx, Data data) throws Exception
  {
    tupleCount.incrementAndGet();
    logger.debug("received {}", data);
    if (firstPayload == null) {
      firstPayload = data;
    }
    else {
      lastPayload = data;
    }
  }
}
