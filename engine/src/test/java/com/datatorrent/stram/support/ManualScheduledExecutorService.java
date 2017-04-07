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
package com.datatorrent.stram.support;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.datatorrent.common.util.ScheduledExecutorService;

/**
 *
 */
public class ManualScheduledExecutorService extends ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  class TimedRunnable
  {
    public Runnable runnable;
    public long time;
  }

  PriorityQueue<TimedRunnable> queue = new PriorityQueue<>(16, new Comparator<TimedRunnable>()
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
