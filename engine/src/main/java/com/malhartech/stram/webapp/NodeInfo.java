/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInfo {
  
  public String id;
  public String name;
  public String containerId;
  public long totalTuples;   
  public long totalBytes;
  public String status;
  public long lastHeartbeat;
  
  public NodeInfo() {
  }

}
