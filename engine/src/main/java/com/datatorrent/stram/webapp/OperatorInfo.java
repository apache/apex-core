/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.datatorrent.api.Stats;
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
  @RecordField(type="meta") public String id;
  @RecordField(type="meta") public String name;
  @RecordField(type="meta") public String className;
  @RecordField(type="stats") public String container; // type=stats because container can change
  @RecordField(type="stats") public String host;
  @RecordField(type="stats") public long totalTuplesProcessed;
  @RecordField(type="stats") public long totalTuplesEmitted;
  @RecordField(type="stats") public long tuplesProcessedPSMA;
  @RecordField(type="stats") public long tuplesEmittedPSMA;
  @RecordField(type="stats") public double cpuPercentageMA;
  @RecordField(type="stats") public long latencyMA;
  public String status;
  public long lastHeartbeat;
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  @RecordField(type="stats") public ArrayList<PortInfo> ports = new ArrayList<PortInfo>();
  @RecordField(type="meta") public String unifierClass;
  @RecordField(type="meta") public String logicalName;
  public long recordingStartTime = Stats.INVALID_TIME_MILLIS;

  /**
   *
   * @param info
   */
  public void addPort(PortInfo info)
  {
    ports.add(info);
  }

  /**
   *
   * @return ArrayList<ContainerInfo>
   *
   */
  public Collection<PortInfo> getPorts()
  {
    return Collections.unmodifiableCollection(ports);
  }

}
