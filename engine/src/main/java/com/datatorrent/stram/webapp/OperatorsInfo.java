/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.webapp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 *
 * @since 0.3.2
 */

@XmlRootElement(name = "operators")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlSeeAlso({OperatorInfo.class})
public class OperatorsInfo {

  protected List<OperatorInfo> operators = new ArrayList<OperatorInfo>();

  /**
   *
   * @param operatorInfo
   */
  public void add(OperatorInfo operatorInfo) {
    operators.add(operatorInfo);
  }

  /**
   *
   * @return list of operator info
   *
   */
  public List<OperatorInfo> getOperators() {
    return Collections.unmodifiableList(operators);
  }

}
