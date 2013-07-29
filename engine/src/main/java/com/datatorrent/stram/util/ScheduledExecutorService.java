/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

/**
 * <p>ScheduledExecutorService interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public interface ScheduledExecutorService extends java.util.concurrent.ScheduledExecutorService
{
    /**
     * 
     * @return long
     */
  public long getCurrentTimeMillis();
}
