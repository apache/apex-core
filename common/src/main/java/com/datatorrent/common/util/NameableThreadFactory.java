/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.common.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class copy-pasted from java.util.concurrent.Executors and modified to meet our needs.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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