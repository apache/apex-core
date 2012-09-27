/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.util.ScheduledExecutorService;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ManualScheduledExecutorService extends ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  class TimedRunnable
  {
    public Runnable runnable;
    public long time;
  }
  PriorityQueue<TimedRunnable> queue = new PriorityQueue<TimedRunnable>(16, new Comparator<TimedRunnable>()
  {
    @Override
    public int compare(TimedRunnable o1, TimedRunnable o2)
    {
      return (int)(o1.time - o2.time);
    }
  });
  long currentTime = 0;

  public ManualScheduledExecutorService(int corePoolSize)
  {
    super(corePoolSize);
  }

  @Override
  public long getCurrentTimeMillis()
  {
    return currentTime;
  }

  public void setCurrentTimeMillis(long current)
  {
    currentTime = current;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
  {
    TimedRunnable tr = new TimedRunnable();
    tr.runnable = command;
    tr.time = getCurrentTimeMillis();
    queue.add(tr);
    return null; // we do not need to worry about this since this is a test
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command, long initialDelay, final long period, TimeUnit unit)
  {
    Runnable scheduler = new Runnable()
    {
      @Override
      public void run()
      {
        command.run();
        TimedRunnable tr = new TimedRunnable();
        tr.runnable = this;
        tr.time = getCurrentTimeMillis() + period;
        queue.add(tr);
      }
    };

    TimedRunnable tr = new TimedRunnable();
    tr.runnable = scheduler;
    tr.time = getCurrentTimeMillis() + initialDelay;
    queue.add(tr);
    return null; // we do not need to worry about this since this is a test
  }

  public void tick(long steps)
  {
    currentTime += steps;

    TimedRunnable tr;
    while ((tr = queue.peek()) != null) {
      if (tr.time > currentTime) {
        break;
      }
      queue.poll();
      tr.runnable.run();
    }
  }
}
