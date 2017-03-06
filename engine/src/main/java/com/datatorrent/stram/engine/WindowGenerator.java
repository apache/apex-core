/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.ScheduledExecutorService;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.ResetWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 *
 * Runs in the hadoop container of the input adapters and generates windows<p>
 * <br>
 * Ensures that all input adapters are sync-ed with the same window size and start. There is one instance
 * of WindowGenerator per hadoop container. All input adapters within a container share it. If a container has
 * no inputadapter, then WindowGenerator instance is a no-op.<br>
 * <br>
 *
 * @since 0.3.2
 */
public class WindowGenerator extends MuxReservoir implements Stream, Runnable
{
  /**
   * corresponds to 2^14 - 1 => maximum bytes needed for varint encoding is 2.
   */
  public static final int WINDOW_MASK = 0x3fff;
  public static final int MIN_WINDOW_ID = 0;
  public static final int MAX_WINDOW_ID = WINDOW_MASK - (WINDOW_MASK % 1000) - 1;
  public static final int MAX_WINDOW_WIDTH = (int)(Long.MAX_VALUE / MAX_WINDOW_ID);
  private final ScheduledExecutorService ses;
  private final BlockingQueue<Tuple> queue;
  private long firstWindowMillis; // Window start time
  private int windowWidthMillis; // Window size
  private long currentWindowMillis;
  private long baseSeconds;
  private int windowId;
  private long resetWindowMillis;
  private int checkPointWindowCount;
  private int checkpointCount = 60; /* default checkpointing after 60 windows */


  public WindowGenerator(ScheduledExecutorService service, int capacity)
  {
    ses = service;
    queue = new CircularBuffer<>(capacity);
  }

  /**
   * Increments window by 1
   */
  public final void advanceWindow()
  {
    currentWindowMillis += windowWidthMillis;
    windowId++;
  }

  private void resetBeginNewWindow() throws InterruptedException
  {
    long timespanBetween2Resets = (long)MAX_WINDOW_ID * windowWidthMillis + windowWidthMillis;
    resetWindowMillis = currentWindowMillis - ((currentWindowMillis - resetWindowMillis) % timespanBetween2Resets);
    windowId = (int)((currentWindowMillis - resetWindowMillis) / windowWidthMillis);

    baseSeconds = (resetWindowMillis / 1000) << 32;
    //logger.info("generating reset -> begin {}", Codec.getStringWindowId(baseSeconds));

    queue.put(new ResetWindowTuple(baseSeconds | windowWidthMillis));
    queue.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
  }

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  private void endCurrentBeginNewWindow() throws InterruptedException
  {
    queue.put(new EndWindowTuple(baseSeconds | windowId));
    if (++checkPointWindowCount == checkpointCount) {
      queue.put(new Tuple(MessageType.CHECKPOINT, baseSeconds | windowId));
      checkPointWindowCount = 0;
    }

    if (windowId == MAX_WINDOW_ID) {
      advanceWindow();
      run();
    } else {
      advanceWindow();
      queue.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
    }
  }

  @Override
  public final void run()
  {
    try {
      resetBeginNewWindow();
    } catch (InterruptedException ie) {
      handleException(ie);
    }
  }

  public void setFirstWindow(long millis)
  {
    firstWindowMillis = millis;
  }

  public void setResetWindow(long millis)
  {
    resetWindowMillis = millis;
  }

  public void setWindowWidth(int millis)
  {
    if (millis > MAX_WINDOW_WIDTH || millis < 1) {
      throw new IllegalArgumentException(String.format("Window width %d is invalid as it's not in the range 1 to %d", millis, MAX_WINDOW_WIDTH));
    }
    windowWidthMillis = millis;
  }

  public void setCheckpointCount(int streamingWindowCount, int offset)
  {
    logger.debug("setCheckpointCount: {} {}", streamingWindowCount, offset);
    checkpointCount = streamingWindowCount;
    checkPointWindowCount = offset;
  }

  @Override
  public void setup(StreamContext context)
  {
    logger.info("WindowGenerator::setup does not do anything useful, please use setFirstWindow/setResetWindow/setWindowWidth do set properties.");
  }

  @Override
  public void activate(StreamContext context)
  {
    currentWindowMillis = firstWindowMillis;

    Runnable subsequentRun = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          endCurrentBeginNewWindow();
        } catch (InterruptedException ie) {
          handleException(ie);
        }
      }

    };

    final long currentTms = ses.getCurrentTimeMillis();
    if (currentWindowMillis < currentTms) {
      logger.info("Catching up from {} to {}", currentWindowMillis, currentTms);
      ses.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                resetBeginNewWindow();
                do {
                  endCurrentBeginNewWindow();
                } while (currentWindowMillis < ses.getCurrentTimeMillis());
              } catch (InterruptedException ie) {
                handleException(ie);
              }
            }
          },
          0, TimeUnit.MILLISECONDS);
    } else {
      logger.info("The input will start to be sliced in {} milliseconds", currentWindowMillis - currentTms);
      ses.schedule(this, currentWindowMillis - currentTms, TimeUnit.MILLISECONDS);
    }

    ses.scheduleAtFixedRate(subsequentRun, currentWindowMillis - currentTms + windowWidthMillis, windowWidthMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void deactivate()
  {
    ses.shutdown();
  }

  private void handleException(Exception e)
  {
    if (e instanceof InterruptedException) {
      ses.shutdown();
    } else {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Object tuple)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected Queue getQueue()
  {
    return queue;
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static long getWindowCount(long millis, long firstMillis, long widthMillis)
  {
    long diff = millis - firstMillis;
    return diff / widthMillis;
  }

  public static long getWindowId(long millis, long firstWindowMillis, long windowWidthMillis)
  {
    long diff = millis - firstWindowMillis;
    long remainder = diff % (windowWidthMillis * (WindowGenerator.MAX_WINDOW_ID + 1));
    long baseSeconds = (millis - remainder) / 1000;
    long windowId = remainder / windowWidthMillis;
    return baseSeconds << 32 | windowId;
  }

  /**
   * @param windowId
   * @param firstWindowMillis
   * @param windowWidthMillis
   * @return the milliseconds for next window.
   */
  public static long getNextWindowMillis(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    return getWindowMillis(windowId, firstWindowMillis, windowWidthMillis) + windowWidthMillis;
  }

  public static long getNextWindowId(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    return getAheadWindowId(windowId, firstWindowMillis, windowWidthMillis, 1);
  }

  public static long getAheadWindowId(long windowId, long firstWindowMillis, long windowWidthMillis, int ahead)
  {
    long millis = getWindowMillis(windowId, firstWindowMillis, windowWidthMillis);
    millis += ahead * windowWidthMillis;
    return getWindowId(millis, firstWindowMillis, windowWidthMillis);
  }

  /**
   * Returns the number of windows windowIdA is ahead of windowIdB.
   *
   * @param windowIdA
   * @param windowIdB
   * @param windowWidthMillis
   * @return the number of windows ahead, negative if windowIdA is behind windowIdB
   */
  public static long compareWindowId(long windowIdA, long windowIdB, long windowWidthMillis)
  {
    // firstWindowMillis here actually does not matter since they will be subtracted out.
    long millisA = getWindowMillis(windowIdA, 0, windowWidthMillis);
    long millisB = getWindowMillis(windowIdB, 0, windowWidthMillis);
    return (millisA - millisB) / windowWidthMillis;
  }

  /**
   * @param windowId
   * @param firstWindowMillis
   * @param windowWidthMillis
   * @return the milliseconds for given window.
   */
  public static long getWindowMillis(long windowId, long firstWindowMillis, long windowWidthMillis)
  {
    if (windowId == -1) {
      return firstWindowMillis;
    }
    long baseMillis = (windowId >> 32) * 1000;
    long diff = baseMillis - firstWindowMillis;
    long baseChangeInterval = windowWidthMillis * (WindowGenerator.MAX_WINDOW_ID + 1);
    assert (baseChangeInterval > 0);
    long multiplier = diff / baseChangeInterval;
    if (diff % baseChangeInterval > 0) {
      multiplier++;
    }
    assert (multiplier >= 0);
    windowId = windowId & WindowGenerator.WINDOW_MASK;
    return firstWindowMillis + (multiplier * baseChangeInterval) + (windowId * windowWidthMillis);
  }

  /**
   * Utility function to get the base seconds from a window id
   *
   * @param windowId
   * @return the base seconds for the given window id
   */
  public static long getBaseSecondsFromWindowId(long windowId)
  {
    return windowId >>> 32;
  }

  private static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
}
