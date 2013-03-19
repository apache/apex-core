/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.bufferserver.packet.ResetRequestTuple;
import com.malhartech.bufferserver.packet.SubscriberRequestTuple;
import com.malhartech.bufferserver.packet.Tuple;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import malhar.netlet.DefaultEventLoop;
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
public class BufferServerSubscriber extends AbstractSocketSubscriber
{
  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriber.class);
  private final String sourceId;
  private final Collection<Integer> partitions;
  private final int mask;
  public AtomicInteger tupleCount = new AtomicInteger(0);
  public Tuple firstPayload, lastPayload;
  public final ArrayList<ResetRequestTuple> resetPayloads = new ArrayList<ResetRequestTuple>();
  public long windowId;

  public BufferServerSubscriber(String sourceId, int mask, Collection<Integer> partitions)
  {
    this.sourceId = sourceId;
    this.mask = mask;
    this.partitions = partitions;
  }

  @Override
  public void activate()
  {
    tupleCount.set(0);
    firstPayload = lastPayload = null;
    resetPayloads.clear();
    super.activate();
    write(SubscriberRequestTuple.getSerializedRequest(
            "BufferServerSubscriber",
            "BufferServerOutput/BufferServerSubscriber",
            sourceId,
            mask,
            partitions,
            windowId));
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
  {
    if (cce instanceof IOException) {
      el.disconnect(this);
    }
    else {
      throw new RuntimeException(cce);
    }
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    byte type = buffer[offset];
    if (type == MessageType.RESET_WINDOW_VALUE) {
      resetPayloads.add(new ResetRequestTuple(buffer, offset, size));
    }
    else {
      tupleCount.incrementAndGet();
      if (firstPayload == null) {
        firstPayload = Tuple.getTuple(buffer, offset, size);
      }
      else if (type == MessageType.BEGIN_WINDOW_VALUE || type == MessageType.RESET_WINDOW_VALUE) {
        lastPayload = Tuple.getTuple(buffer, offset, size);
      }
    }
  }

}
