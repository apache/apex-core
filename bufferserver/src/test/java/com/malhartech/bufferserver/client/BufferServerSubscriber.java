/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.SubscribeRequestTuple;
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
  public WindowIdHolder firstPayload, lastPayload;
  public final ArrayList<Object> resetPayloads = new ArrayList<Object>();
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
    write(SubscribeRequestTuple.getSerializedRequest(
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
    Tuple tuple = Tuple.getTuple(buffer, offset, size);
    tupleCount.incrementAndGet();
    switch (tuple.getType()) {
      case BEGIN_WINDOW:
        beginWindow(tuple.getWindowId());
        break;

      case END_WINDOW:
        endWindow(tuple.getWindowId());
        break;

      case RESET_WINDOW:
        resetWindow(tuple.getBaseSeconds(), tuple.getWindowWidth());
        break;
    }
  }

  public void beginWindow(final int windowId)
  {
    WindowIdHolder payload = new WindowIdHolder() {

      @Override
      public int getWindowId()
      {
        return windowId;
      }
    };

    if (firstPayload == null) {
      firstPayload = payload;
    }

    lastPayload = payload;
  }

  public void endWindow(final int windowId)
  {
    WindowIdHolder payload = new WindowIdHolder() {

      @Override
      public int getWindowId()
      {
        return windowId;
      }
    };

    if (firstPayload == null) {
      firstPayload = payload;
    }

    lastPayload = payload;
  }

  public void resetWindow(int baseSeconds, int windowWidth)
  {
  }

  @Override
  public String toString()
  {
    return "BufferServerSubscriber";
  }

  public interface WindowIdHolder
  {
    public int getWindowId();
  }

}
