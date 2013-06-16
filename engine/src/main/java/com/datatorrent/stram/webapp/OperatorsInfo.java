/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 *
 * Provides plan level operator data<p>
 * <br>
 * This call provides restful access to individual operator instance data<br>
 * <br>
 */

@XmlRootElement(name = "operators")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({OperatorInfo.class})
public class OperatorsInfo {

  protected ArrayList<OperatorInfo> operators = new ArrayList<OperatorInfo>();

  /**
   *
   * @param operatorInfo
   */
  public void add(OperatorInfo operatorInfo) {
    operators.add(operatorInfo);
  }

  /**
   *
   * @return ArrayList<OperatorInfo>
   *
   */
  public ArrayList<OperatorInfo> getOperators() {
    return operators;
  }

}
