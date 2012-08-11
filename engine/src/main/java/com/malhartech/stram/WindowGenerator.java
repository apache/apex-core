/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.InputAdapter;
import java.util.Collection;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowGenerator implements Runnable
{
  public static final Logger logger = LoggerFactory.getLogger(WindowGenerator.class);
  private final long startMillis; // Window start time
  private final int intervalMillis; // Window size
  private long currentWindowMillis = -1;
  private final Collection<? extends InputAdapter> inputAdapters;
  private ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1);
  private int windowId;

  public WindowGenerator(Collection<? extends InputAdapter> inputs, long startMillis, int intervalMillis)
  {
    this.inputAdapters = inputs;
    this.startMillis = startMillis;
    this.intervalMillis = intervalMillis;
    this.currentWindowMillis = this.startMillis;
  }

  public final void advanceWindow()
  {
    currentWindowMillis += intervalMillis;
    windowId++;
  }

  protected final void nextWindow()
  {
    if (windowId == InputAdapter.MAX_VALUE_WINDOW) {
//      logger.debug("generating end -> reset window {}", Integer.toHexString(windowId));
      for (InputAdapter ia : inputAdapters) {
        ia.endWindow(windowId);
      }
      advanceWindow();
      run();
    }
    else {
//      logger.debug("generating end -> begin {}", Integer.toHexString(windowId));
      int previousWindowId = windowId;
      advanceWindow();
      for (InputAdapter ia : inputAdapters) {
        ia.endWindow(previousWindowId);
        ia.beginWindow(windowId);
      }
    }
  }

  @Override
  public void run()
  {
    windowId = 0;
//    logger.debug("generating reset -> begin {}", Long.toHexString(currentWindowMillis));
    int baseSeconds = (int) (currentWindowMillis / 1000);
    for (InputAdapter ia : inputAdapters) {
      ia.resetWindow(baseSeconds, intervalMillis);
      ia.beginWindow(windowId);
    }
  }

  public void start()
  {
    Runnable subsequentRun = new Runnable()
    {
      @Override
      public void run()
      {
        nextWindow();
      }
    };

    long currentTms = System.currentTimeMillis();

    if (currentWindowMillis < currentTms) {
      run();
      do {
        nextWindow();
      } while (currentWindowMillis < currentTms);
    }
    else {
      stpe.schedule(this, currentWindowMillis - currentTms, TimeUnit.MILLISECONDS);
    }

    stpe.scheduleAtFixedRate(subsequentRun, currentWindowMillis - currentTms + intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
  }

  public void stop()
  {
    stpe.shutdown();
  }
}