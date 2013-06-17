/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.support;

import com.datatorrent.bufferserver.packet.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Subscriber extends com.datatorrent.bufferserver.client.Subscriber
{
  public final ArrayList<Object> resetPayloads = new ArrayList<Object>();
  public AtomicInteger tupleCount = new AtomicInteger(0);
  public WindowIdHolder firstPayload, lastPayload;

  public Subscriber(String id)
  {
    super(id);
  }

  @Override
  public void activate(String version, String type, String sourceId, int mask, Collection<Integer> partitions, long windowId)
  {
    tupleCount.set(0);
    firstPayload = lastPayload = null;
    resetPayloads.clear();
    super.activate(version, type, sourceId, mask, partitions, windowId);
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
    WindowIdHolder payload = new WindowIdHolder()
    {
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
    WindowIdHolder payload = new WindowIdHolder()
    {
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

  public void resetWindow(final int baseSeconds, final int windowWidth)
  {
    resetPayloads.add(new ResetHolder()
    {
      @Override
      public int getBaseSeconds()
      {
        return baseSeconds;
      }

      @Override
      public int getWindowWidth()
      {
        return windowWidth;
      }

    });
  }

  public interface WindowIdHolder
  {
    public int getWindowId();

  }

  public interface ResetHolder
  {
    public int getBaseSeconds();

    public int getWindowWidth();

  }

  @Override
  public String toString()
  {
    return "BufferServerSubscriber";
  }

  private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);
}
