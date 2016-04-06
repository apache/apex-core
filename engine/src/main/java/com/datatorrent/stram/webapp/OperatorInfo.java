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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.datatorrent.api.annotation.RecordField;

/**
 *
 * Provides operator instance level data like id, container id, throughput, etc.<p>
 * <br>Current data includes<br>
 * <b>Operator Id</b><br>
 * <b>Operator Name</b><br>
 * <b>Container Id</b><br>
 * <b>Total Tuples processed</b><br>
 * <b>Total Tuples produced</b><br>
 * <b>Operator Status</b><br>
 * <b>Last Heartbeat Time</b><br>
 * <br>
 *
 * @since 0.3.2
 */
@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class OperatorInfo
{
  @RecordField(type = "meta")
  public String id;
  @RecordField(type = "meta")
  public String name;
  @RecordField(type = "meta", publish = false)
  public String className;
  @RecordField(type = "stats")
  public String container; // type=stats because container can change
  @RecordField(type = "stats")
  public String host;
  @RecordField(type = "stats")
  public long totalTuplesProcessed;
  @RecordField(type = "stats")
  public long totalTuplesEmitted;
  @RecordField(type = "stats")
  public long tuplesProcessedPSMA;
  @RecordField(type = "stats")
  public long tuplesEmittedPSMA;
  @RecordField(type = "stats")
  public double cpuPercentageMA;
  @RecordField(type = "stats")
  public long latencyMA;
  public String status;
  public long lastHeartbeat;
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  @RecordField(type = "stats")
  public List<PortInfo> ports = new ArrayList<>();
  @RecordField(type = "meta")
  public String unifierClass;
  @RecordField(type = "meta")
  public String logicalName;
  public String recordingId;
  @RecordField(type = "stats")
  public Object counters;
  @RecordField(type = "stats")
  public Map<String, Object> metrics;
  @RecordField(type = "stats")
  public long checkpointStartTime;
  @RecordField(type = "stats")
  public long checkpointTime;
  @RecordField(type = "stats")
  public long checkpointTimeMA;

  /**
   * @param info
   */
  public void addPort(PortInfo info)
  {
    ports.add(info);
  }

  /**
   * @return
   */
  public Collection<PortInfo> getPorts()
  {
    return Collections.unmodifiableCollection(ports);
  }

}
