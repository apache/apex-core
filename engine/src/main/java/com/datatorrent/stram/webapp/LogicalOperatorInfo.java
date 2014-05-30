/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.webapp;

import com.datatorrent.api.Context.Counters;
import java.util.*;
import javax.xml.bind.annotation.*;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * <p>LogicalOperatorInfo class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.5
 */
@XmlRootElement(name = "logicalOperator")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogicalOperatorInfo
{
  public String name;
  public String className;
  public long totalTuplesProcessed = 0;
  public long totalTuplesEmitted = 0;
  public long tuplesProcessedPSMA;
  public long tuplesEmittedPSMA;
  public double cpuPercentageMA;
  public long latencyMA;
  public Map<String, MutableInt> status;
  public long lastHeartbeat;
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  public Set<String> containerIds;
  public Set<Integer> partitions;
  public Set<Integer> unifiers;
  public Set<String> hosts;
  public List<Counters> counters;
}
