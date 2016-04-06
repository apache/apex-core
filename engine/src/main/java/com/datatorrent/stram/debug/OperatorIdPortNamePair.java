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
package com.datatorrent.stram.debug;

import com.datatorrent.api.Component;

/**
 *
 */
class OperatorIdPortNamePair
{
  public final int operatorId;
  public final String portName;

  OperatorIdPortNamePair(int operatorId, String portName)
  {
    this.operatorId = operatorId;
    this.portName = portName;
  }

  @Override
  public String toString()
  {
    if (portName == null) {
      return String.valueOf(operatorId);
    } else {
      return String.valueOf(operatorId).concat(Component.CONCAT_SEPARATOR).concat(portName);
    }
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 43 * hash + this.operatorId;
    hash = 43 * hash + (this.portName != null ? this.portName.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OperatorIdPortNamePair other = (OperatorIdPortNamePair)obj;
    if (this.operatorId != other.operatorId) {
      return false;
    }
    if ((this.portName == null) ? (other.portName != null) : !this.portName.equals(other.portName)) {
      return false;
    }
    return true;
  }

}
