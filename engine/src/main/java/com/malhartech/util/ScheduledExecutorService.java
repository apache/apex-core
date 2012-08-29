/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface ScheduledExecutorService extends java.util.concurrent.ScheduledExecutorService
{
  public long getCurrentTimeMillis();
}
