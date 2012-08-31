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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
  public static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
  public static final String FIRST_WINDOW_MILLIS = "FirstWindowMillis";
  public static final String WINDOW_WIDTH_MILLIS = "WindowWidthMillis";
  /**
   * corresponds to 2^14 - 1 => maximum bytes needed for varint encoding is 2.
   */
  public static final int MAX_VALUE_WINDOW = 0x3fff - (0x3fff % 1000) - 1;
  private final ScheduledExecutorService ses;
  private long startMillis; // Window start time
  private int intervalMillis; // Window size
  HashMap<String, Sink> outputs = new HashMap<String, Sink>();
  volatile Collection<Sink> sinks;
  private long currentWindowMillis = -1;
  private long baseSeconds;
  private int windowId;

  public WindowGenerator(ScheduledExecutorService service)
  {
    ses = service;
  }
  
  /**
   * Increments window by 1
   */
  public final void advanceWindow()
  {
    currentWindowMillis += intervalMillis;
    windowId++;
  }

  /**
   * Updates window in a circular buffer on inputAdapters<p>
   * This code generates the windows
   */
  protected final void nextWindow()
  {
    if (windowId == MAX_VALUE_WINDOW) {
      EndWindowTuple t = new EndWindowTuple();
      t.setWindowId(windowId);
      for (Sink s: sinks) {
        s.process(t);
      }

      advanceWindow();
      run();
    }
    else {
//      logger.debug("generating end -> begin {}", Integer.toHexString(windowId));
      int previousWindowId = windowId;
      advanceWindow();

      EndWindowTuple ewt = new EndWindowTuple();
      ewt.setWindowId(previousWindowId);

      Tuple bwt = new Tuple(Buffer.Data.DataType.BEGIN_WINDOW);
      bwt.setWindowId(baseSeconds | windowId);
      for (Sink s: sinks) {
        s.process(ewt);
        s.process(bwt);
      }
    }
  }

  /**
   *
   */
  @Override
  public void run()
  {
    windowId = 0;
//    logger.debug("generating reset -> begin {}", Long.toHexString(currentWindowMillis));
    baseSeconds = (currentWindowMillis / 1000) << 32;

    ResetWindowTuple rwt = new ResetWindowTuple();
    rwt.setWindowId(baseSeconds | intervalMillis);

    Tuple t = new Tuple(Buffer.Data.DataType.BEGIN_WINDOW);
    t.setWindowId(baseSeconds | windowId);

    for (Sink s: sinks) {
      s.process(rwt);
      s.process(t);
    }
  }

  @Override
  public void setup(Configuration config)
  {
    startMillis = config.getLong(FIRST_WINDOW_MILLIS, ses.getCurrentTimeMillis());
    intervalMillis = config.getInt(WINDOW_WIDTH_MILLIS, 500);
  }

  @Override
  public void activate(Context context)
  {
    sinks = outputs.values();
    currentWindowMillis = startMillis;

    Runnable subsequentRun = new Runnable()
    {
      @Override
      public void run()
      {
        nextWindow();
      }
    };

    long currentTms = ses.getCurrentTimeMillis();

    if (currentWindowMillis < currentTms) {
      run();
      do {
        nextWindow();
      }
      while (currentWindowMillis < currentTms);
    }
    else {
      ses.schedule(this, currentWindowMillis - currentTms, TimeUnit.MILLISECONDS);
    }

    ses.scheduleAtFixedRate(subsequentRun, currentWindowMillis - currentTms + intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void deactivate()
  {
    sinks = Collections.emptyList();
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
    outputs.put(id, component);
    return null;
  }

  @Override
  public void process(Object payload)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
