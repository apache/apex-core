/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram.webapp;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 
 * Provides dag level node data<p>
 * <br>
 * This call provides restful access to individual node data<br>
 * <br>
 */

@XmlRootElement(name = "nodes")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodesInfo {

  protected ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();

  /**
   * 
   * @param nodeInfo 
   */
  public void add(NodeInfo nodeInfo) {
    nodes.add(nodeInfo);
  }

  /**
   * 
   * @return ArrayList<NodeInfo>
   * 
   */
  public ArrayList<NodeInfo> getNodes() {
    return nodes;
  }

}
