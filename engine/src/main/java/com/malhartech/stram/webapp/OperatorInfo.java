/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import com.malhartech.api.annotation.RecordField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

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
 */
@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class OperatorInfo
{
  @RecordField(type="meta") public String id;
  @RecordField(type="meta") public String name;
  @RecordField(type="stats") public String container;
  @RecordField(type="stats") public String host;
  @RecordField(type="stats") public long totalTuplesProcessed;
  @RecordField(type="stats") public long totalTuplesEmitted;
  @RecordField(type="stats") public long tuplesProcessedPSMA10;
  @RecordField(type="stats") public long tuplesEmittedPSMA10;
  @RecordField(type="stats") public double cpuPercentageMA10;
  @RecordField(type="stats") public long latencyMA;
  public String status;
  public long lastHeartbeat;
  public long failureCount;
  public long recoveryWindowId;
  public long currentWindowId;
  public List<String> recordingNames; // null if recording is not happening
  protected ArrayList<PortInfo> inputPorts = new ArrayList<PortInfo>();
  protected ArrayList<PortInfo> outputPorts = new ArrayList<PortInfo>();

  /**
   *
   * @param info
   */
  public void addInputPort(PortInfo info)
  {
    inputPorts.add(info);
  }

  /**
   *
   * @param info
   */
  public void addOutputPort(PortInfo info)
  {
    outputPorts.add(info);
  }

  /**
   *
   * @return ArrayList<ContainerInfo>
   *
   */
  public Collection<PortInfo> getInputPorts()
  {
    return Collections.unmodifiableCollection(inputPorts);
  }

  /**
   *
   * @return ArrayList<ContainerInfo>
   *
   */
  public Collection<PortInfo> getOutputPorts()
  {
    return Collections.unmodifiableCollection(outputPorts);
  }

  public OperatorInfo()
  {
  }

}
