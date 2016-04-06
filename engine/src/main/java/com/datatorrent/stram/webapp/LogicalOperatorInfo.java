/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.webapp;

import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.api.AutoMetric;

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
  @AutoMetric
  public long totalTuplesProcessed = 0;
  @AutoMetric
  public long totalTuplesEmitted = 0;
  @AutoMetric
  public long tuplesProcessedPSMA;
  @AutoMetric
  public long tuplesEmittedPSMA;
  @AutoMetric
  public double cpuPercentageMA;
  @AutoMetric
  public long latencyMA;
  public Map<String, MutableInt> status;
  public long lastHeartbeat;
  @AutoMetric
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  public Set<String> containerIds;
  public Set<Integer> partitions;
  public Set<Integer> unifiers;
  public Set<String> hosts;
  public Object counters;
  public Object autoMetrics;
  @AutoMetric
  public long checkpointTimeMA;
}
