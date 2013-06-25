/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import com.datatorrent.bufferserver.util.NameableThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class ScheduledThreadPoolExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  public static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorService.class);

  public ScheduledThreadPoolExecutor(int corePoolSize, String executorName)
  {
    super(corePoolSize, new NameableThreadFactory(executorName));
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
