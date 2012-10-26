/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Stats
{
  public interface StatsReporter
  {
    public Stats getStats(String id);
  }

  public interface Counter
  {
    public int getCount();

    public int resetCount();
  }
}
