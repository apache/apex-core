/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.webapp;

import com.datatorrent.api.NumberAggregate.*;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class OperatorAggregationInfo
{
  public String name;
  public LongAggregate tuplesProcessedPSMA = new LongAggregate();
  public LongAggregate tuplesEmittedPSMA = new LongAggregate();
  public DoubleAggregate cpuPercentageMA = new DoubleAggregate();
  public LongAggregate latencyMA = new LongAggregate(true, false);
  public LongAggregate lastHeartbeat = new LongAggregate(true, true);
  public LongAggregate failureCount = new LongAggregate();
  public LongAggregate recoveryWindowId = new LongAggregate(true, true);
  public LongAggregate currentWindowId = new LongAggregate(true, true);
  public Object counters;
}
