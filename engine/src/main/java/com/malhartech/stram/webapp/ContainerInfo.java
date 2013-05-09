/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import com.malhartech.annotation.RecordField;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
public class ContainerInfo {
  @RecordField(type="meta") public String id;
  @RecordField(type="meta") public String host;
  public String state;
  @RecordField(type="meta") public String jvmName;
  public long lastHeartbeat;
  @RecordField(type="stats") public int numOperators;
  @RecordField(type="meta") public int memoryMBAllocated;
  @RecordField(type="stats") public int memoryMBFree;
}
