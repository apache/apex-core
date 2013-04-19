/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.bufferserver.packet.MessageType;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.ResetWindowTuple;
import com.malhartech.tuple.Tuple;
import com.malhartech.util.CircularBuffer;
import com.malhartech.util.ScheduledExecutorService;
import java.util.HashMap;
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
 */
public class WindowGenerator implements Stream<Object>, Runnable
{
  /**
   * corresponds to 2^14 - 1 => maximum bytes needed for varint encoding is 2.
   */
  private final CircularBuffer<Tuple> controlTuples = new CircularBuffer<Tuple>(1024);
  public static final int WINDOW_MASK = 0x3fff;
  public static final int MAX_WINDOW_ID = WINDOW_MASK - (WINDOW_MASK % 1000) - 1;
  public static final int MAX_WINDOW_WIDTH = (int)(Long.MAX_VALUE / MAX_WINDOW_ID) > 0 ? (int)(Long.MAX_VALUE / MAX_WINDOW_ID) : Integer.MAX_VALUE;
  private final ScheduledExecutorService ses;
  private long firstWindowMillis; // Window start time
  private int windowWidthMillis; // Window size
  private long currentWindowMillis;
  private long baseSeconds;
  private int windowId;
  private long resetWindowMillis;

  public WindowGenerator(ScheduledExecutorService service)
  {
    ses = service;
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

    controlTuples.put(new ResetWindowTuple(baseSeconds | windowWidthMillis));
    controlTuples.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
  }

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  private void endCurrentBeginNewWindow() throws InterruptedException
  {
    if (windowId == MAX_WINDOW_ID) {
      controlTuples.put(new EndWindowTuple(baseSeconds | windowId));
      advanceWindow();
      run();
    }
    else {
//      logger.debug("generating end -> begin {}", Integer.toHexString(windowId));
      controlTuples.put(new EndWindowTuple(baseSeconds | windowId));
      advanceWindow();

      controlTuples.put(new Tuple(MessageType.BEGIN_WINDOW, baseSeconds | windowId));
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
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  @Override
  public void process(Object tuple)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @SuppressWarnings("VolatileArrayField")
  private volatile BufferReservoir[] reservoirs = new BufferReservoir[0];
  private HashMap<String, BufferReservoir> reservoirMap = new HashMap<String, BufferReservoir>();

  @Override
  public Reservoir getReservoir(String sinkId)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  class BufferReservoir extends CircularBuffer<Tuple> implements Reservoir
  {
    long count;

    BufferReservoir()
    {
      super(controlTuples.capacity());
    }

    @Override
    public void setSink(Sink<Object> sink)
    {
      logger.info("Unnecessary call to setSink on Reservoir of WindowGenerator");
    }

    @Override
    public Tuple sweep()
    {
      if (!isEmpty()) {
        return peekUnsafe();
      }

      synchronized (controlTuples) {
        /* find out the minimum remaining capacity in all the other buffers and consume those many tuples from bufferserver */
        int min = controlTuples.size();
        if (min == 0) {
          return null;
        }

        for (int i = reservoirs.length; i-- > 0;) {
          if (reservoirs[i].remainingCapacity() < min) {
            min = reservoirs[i].remainingCapacity();
          }
        }

        while (min-- > 0) {
          Tuple t = controlTuples.remove();
          for (int i = reservoirs.length; i-- > 0;) {
            reservoirs[i].add(t);
          }
        }
      }

      return null;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
}
