/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.stram.appdata.AppDataPushAgent;
import java.util.*;
import javax.xml.bind.annotation.*;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * <p>LogicalOperatorInfo class.</p>
 *
 * @since 0.9.5
 */
@XmlRootElement(name = "logicalOperator")
@XmlAccessorType(XmlAccessType.FIELD)
public class LogicalOperatorInfo
{
  public String name;
  public String className;
  @AppDataPushAgent.Metric
  public long totalTuplesProcessed = 0;
  @AppDataPushAgent.Metric
  public long totalTuplesEmitted = 0;
  @AppDataPushAgent.Metric
  public long tuplesProcessedPSMA;
  @AppDataPushAgent.Metric
  public long tuplesEmittedPSMA;
  @AppDataPushAgent.Metric
  public double cpuPercentageMA;
  @AppDataPushAgent.Metric
  public long latencyMA;
  public Map<String, MutableInt> status;
  public long lastHeartbeat;
  @AppDataPushAgent.Metric
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  public Set<String> containerIds;
  public Set<Integer> partitions;
  public Set<Integer> unifiers;
  public Set<String> hosts;
  public Object counters;
  public Object customMetrics;
  @AppDataPushAgent.Metric
  public long checkpointTimeMA;
}
