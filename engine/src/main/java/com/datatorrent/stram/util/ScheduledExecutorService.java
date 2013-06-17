/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface ScheduledExecutorService extends java.util.concurrent.ScheduledExecutorService
{
    /**
     * 
     * @return long
     */
  public long getCurrentTimeMillis();
}
