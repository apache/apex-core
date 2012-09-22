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
 * Provides plan level operator data<p>
 * <br>
 * This call provides restful access to individual operator instance data<br>
 * <br>
 */

@XmlRootElement(name = "operators")
@XmlAccessorType(XmlAccessType.FIELD)
public class OperatorsInfo {

  protected ArrayList<OperatorInfo> nodes = new ArrayList<OperatorInfo>();

  /**
   *
   * @param operatorInfo
   */
  public void add(OperatorInfo operatorInfo) {
    nodes.add(operatorInfo);
  }

  /**
   *
   * @return ArrayList<OperatorInfo>
   *
   */
  public ArrayList<OperatorInfo> getOperators() {
    return nodes;
  }

}
