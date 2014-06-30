/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.webapp;

import java.util.*;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class OperatorAggregationInfo
{
  public static enum Type { MIN, MAX, AVG, SUM };
  public String name;
  public EnumMap<Type, Long> tuplesProcessedPSMA = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Long> tuplesEmittedPSMA = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Double> cpuPercentageMA = new EnumMap<Type, Double>(Type.class);
  public EnumMap<Type, Long> latencyMA = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Long> lastHeartbeat = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Long> failureCount = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Long> recoveryWindowId = new EnumMap<Type, Long>(Type.class);
  public EnumMap<Type, Long> currentWindowId = new EnumMap<Type, Long>(Type.class);
  public Object counters;
}
