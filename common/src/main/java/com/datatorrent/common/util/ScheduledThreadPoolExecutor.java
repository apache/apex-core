/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ScheduledThreadPoolExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements ScheduledExecutorService
{
  public ScheduledThreadPoolExecutor(int corePoolSize)
  {
    super(corePoolSize);
  }

  @Override
  public final long getCurrentTimeMillis()
  {
    return System.currentTimeMillis();
  }
}
