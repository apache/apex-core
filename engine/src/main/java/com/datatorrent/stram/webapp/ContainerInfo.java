
/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 *
 * @since 0.3.2
 */
package com.datatorrent.stram.webapp;

import com.datatorrent.api.annotation.RecordField;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {
  @RecordField(type = "meta")
  public String id;
  @RecordField(type = "meta", publish = false)
  public String host;
  public String state;
  @RecordField(type = "meta", publish = false)
  public String jvmName;
  public long lastHeartbeat;
  @RecordField(type = "stats")
  public int numOperators;
  @RecordField(type = "meta")
  public int memoryMBAllocated;
  @RecordField(type = "stats")
  public int memoryMBFree;
  public String containerLogsUrl;
  public long startedTime = -1;
  public long finishedTime = -1;
  @RecordField(type = "meta", publish = false)
  public String rawContainerLogsUrl;
}
