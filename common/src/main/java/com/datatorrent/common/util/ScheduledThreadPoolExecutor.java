/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ScheduledThreadPoolExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  public static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorService.class);

  /**
   * Class copy-pasted from java.util.concurrent.Executors and modified to meet our needs.
   */
  static class DefaultThreadFactory implements ThreadFactory
  {
    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    DefaultThreadFactory(String groupname)
    {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup()
              : Thread.currentThread().getThreadGroup();
      namePrefix = groupname
              + "-"
              + poolNumber.getAndIncrement()
              + "-";
    }

    @Override
    public Thread newThread(Runnable r)
    {
      Thread t = new Thread(group, r,
                            namePrefix + threadNumber.getAndIncrement(),
                            0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

  public ScheduledThreadPoolExecutor(int corePoolSize, String executorName)
  {
    super(corePoolSize, new DefaultThreadFactory(executorName));
  }

  /**
   *
   * @return long
   */
  @Override
  public final long getCurrentTimeMillis()
  {
    return System.currentTimeMillis();
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t)
  {
    super.afterExecute(r, t);
    if (t != null) {
      logger.error("Scheduled task {} died with {}", r, t);
    }
  }
}
