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
public class LogicalOperatorsInfo
{

  protected List<LogicalOperatorInfo> operators = new ArrayList<>();

  /**
   * @param operatorInfo
   */
  public void add(LogicalOperatorInfo operatorInfo)
  {
    operators.add(operatorInfo);
  }

  /**
   * @return list of operator info
   */
  public List<LogicalOperatorInfo> getOperators()
  {
    return Collections.unmodifiableList(operators);
  }

}
