/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.stram.engine.StreamContext;

/**
 * <p>FastSubscriber class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class FastSubscriber extends BufferServerSubscriber
{
  public FastSubscriber(String id, int queueCapacity)
  {
    super(id, queueCapacity);
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getFinishedWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activate(Tuple.FAST_VERSION, context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getFinishedWindowId(), freeFragments.capacity());
  }

  @Override
  public int readSize()
  {
    if (writeOffset - readOffset < 2) {
      return -1;
    }

    short s = buffer[readOffset++];
    return s | (buffer[readOffset++] << 8);
  }

  private static final Logger logger = LoggerFactory.getLogger(FastSubscriber.class);
}
