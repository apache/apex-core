/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.api.Stats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;

/**
 * <p>HeartbeatListener interface.</p>
 *
 * @param <STATS> Type of stats which this stats listener handles.
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public interface StatsListener<STATS extends Stats>
{
  public void collected(STATS stats);

  public interface ContainerStatsListener extends StatsListener<ContainerStats>
  {
  }

}
