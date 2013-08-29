/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.stram.util.ScheduledExecutorService;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.ResetWindowTuple;
import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.netlet.util.CircularBuffer;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final int MAX_WINDOW_ID = WINDOW_MASK - (WINDOW_MASK % 1000) - 1;
  public static final int MAX_WINDOW_WIDTH = (int)(Long.MAX_VALUE / MAX_WINDOW_ID) > 0 ? (int)(Long.MAX_VALUE / MAX_WINDOW_ID) : Integer.MAX_VALUE;
  private final ScheduledExecutorService ses;
  private final MasterReservoir masterReservoir;
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
    masterReservoir = new MasterReservoir(capacity);
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

    masterReservoir.put(new ResetWindowTuple(baseSeconds | windowWidthMillis));
    masterReservoir.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
  }

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  private void endCurrentBeginNewWindow() throws InterruptedException
  {
    masterReservoir.put(new EndWindowTuple(baseSeconds | windowId));
    if (++checkPointWindowCount == checkpointCount) {
      masterReservoir.put(new Tuple(MessageType.CHECKPOINT, baseSeconds | windowId));
      checkPointWindowCount = 0;
    }

    if (windowId == MAX_WINDOW_ID) {
      advanceWindow();
      run();
    }
    else {
      advanceWindow();
      masterReservoir.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
    }
  }

  @Override
  public final void run()
  {
    try {
      resetBeginNewWindow();
    }
    catch (InterruptedException ie) {
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

  public void setCheckpointCount(int streamingWindowCount)
  {
    checkpointCount = streamingWindowCount;
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
        }
        catch (InterruptedException ie) {
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
                    }
                    while (currentWindowMillis < ses.getCurrentTimeMillis());
                  }
                  catch (InterruptedException ie) {
                    handleException(ie);
                  }
                }

              },
              0, TimeUnit.MILLISECONDS);
    }
    else {
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
    }
    else {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  @Override
  public void put(Object tuple)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public Reservoir getMasterReservoir()
  {
    return masterReservoir;
  }

  @Override
  public int getCount(boolean reset)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private class MasterReservoir extends CircularBuffer<Tuple> implements Reservoir
  {
    MasterReservoir(int n)
    {
      super(n);
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
}
