/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.dag.Component;
import com.malhartech.dag.Context;
import com.malhartech.dag.EndWindowTuple;
import com.malhartech.dag.ResetWindowTuple;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.util.ScheduledExecutorService;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
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
public class WindowGenerator implements Component<Configuration, Context>, Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
  public static final String FIRST_WINDOW_MILLIS = "FirstWindowMillis";
  public static final String WINDOW_WIDTH_MILLIS = "WindowWidthMillis";
  public static final String RESET_WINDOW_MILLIS = "ResetWindowMillis";
  /**
   * corresponds to 2^14 - 1 => maximum bytes needed for varint encoding is 2.
   */
  public static final int MAX_WINDOW_ID = 0x3fff - (0x3fff % 1000) - 1;
  public static final int MAX_WINDOW_WIDTH = (int)(Long.MAX_VALUE / MAX_WINDOW_ID);
  private final ScheduledExecutorService ses;
  private long firstWindowMillis; // Window start time
  private int windowWidthMillis; // Window size
  HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink[] sinks = NO_SINKS;
  private long currentWindowMillis = -1;
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

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  protected final void nextWindow()
  {
    if (windowId == MAX_WINDOW_ID) {
      EndWindowTuple t = new EndWindowTuple();
      t.setWindowId(baseSeconds | windowId);
      for (Sink s: sinks) {
        s.process(t);
      }

      advanceWindow();
      run();
    }
    else {
//      logger.debug("generating end -> begin {}", Integer.toHexString(windowId));
      EndWindowTuple ewt = new EndWindowTuple();
      ewt.setWindowId(baseSeconds | windowId);
      for (int i = sinks.length; i-- > 0;) {
        sinks[i].process(ewt);
      }

      advanceWindow();

      Tuple bwt = new Tuple(Buffer.Data.DataType.BEGIN_WINDOW);
      bwt.setWindowId(baseSeconds | windowId);
      for (int i = sinks.length; i-- > 0;) {
        sinks[i].process(bwt);
      }
    }
  }

  /**
   *
   */
  @Override
  public void run()
  {
    long timespanBetween2Resets = (long)MAX_WINDOW_ID * windowWidthMillis + windowWidthMillis;
    resetWindowMillis = firstWindowMillis - ((firstWindowMillis - resetWindowMillis) % timespanBetween2Resets);
    windowId = (int)((firstWindowMillis - resetWindowMillis) / windowWidthMillis);

    //    logger.debug("generating reset -> begin {}", Long.toHexString(currentWindowMillis));

    baseSeconds = (resetWindowMillis / 1000) << 32;
    ResetWindowTuple rwt = new ResetWindowTuple();
    rwt.setWindowId(baseSeconds | windowWidthMillis);

    Tuple bwt = new Tuple(Buffer.Data.DataType.BEGIN_WINDOW);
    bwt.setWindowId(baseSeconds | windowId);

    /**
     * we do two separate loops to ensure that we do not end up sending the same tuple twice to a single sink.
     */
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(rwt);
    }
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(bwt);
    }
  }

  @Override
  public void setup(Configuration config)
  {
    firstWindowMillis = config.getLong(FIRST_WINDOW_MILLIS, ses.getCurrentTimeMillis());
    windowWidthMillis = config.getInt(WINDOW_WIDTH_MILLIS, 500);
    if (windowWidthMillis > MAX_WINDOW_WIDTH || windowWidthMillis < 1) {
      throw new IllegalArgumentException(String.format("Window width %d is invalid as it's not in the range 1 to %d", windowWidthMillis, MAX_WINDOW_WIDTH));
    }
    resetWindowMillis = config.getLong(RESET_WINDOW_MILLIS, firstWindowMillis);
  }

  @Override
  public void activate(Context context)
  {
    activateSinks();

    currentWindowMillis = firstWindowMillis;

    Runnable subsequentRun = new Runnable()
    {
      @Override
      public void run()
      {
        nextWindow();
      }
    };

    final long currentTms = ses.getCurrentTimeMillis();
    if (currentWindowMillis < currentTms) {
      ses.schedule(
              new Runnable()
              {
                @Override
                public void run()
                {
                  WindowGenerator.this.run();
                  do {
                    nextWindow();
                  }
                  while (currentWindowMillis < currentTms);
                }
              },
              0, TimeUnit.MILLISECONDS);
    }
    else {
      ses.schedule(this, currentWindowMillis - currentTms, TimeUnit.MILLISECONDS);
    }

    ses.scheduleAtFixedRate(subsequentRun, currentWindowMillis - currentTms + windowWidthMillis, windowWidthMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void deactivate()
  {
    sinks = NO_SINKS;
    ses.shutdown();
  }

  @Override
  public void teardown()
  {
    outputs.clear();
  }

  @Override
  public Sink connect(String id, Sink component)
  {
    if (component == null) {
      outputs.remove(id);
    }
    else {
      outputs.put(id, component);
    }
    if (sinks != NO_SINKS) {
      activateSinks();
    }
    return null;
  }

  @Override
  public void process(Object payload)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public Set<String> getOutputIds()
  {
    return outputs.keySet();
  }

  @SuppressWarnings("SillyAssignment")
  private void activateSinks()
  {
    sinks = new Sink[outputs.size()];
    int i = 0;
    for (Sink s: outputs.values()) {
      sinks[i++] = s;
    }
    sinks = sinks;
  }
}
