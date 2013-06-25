/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
/**
 * Class copy-pasted from java.util.concurrent.Executors and modified to meet our needs.
 */
public class NameableThreadFactory implements ThreadFactory
{
  private static final AtomicInteger poolNumber = new AtomicInteger(1);
  private final ThreadGroup group;
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String namePrefix;

  public NameableThreadFactory(String groupname)
  {
    SecurityManager s = java.lang.System.getSecurityManager();
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
