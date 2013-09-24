/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.debug;

import com.datatorrent.api.Component;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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
    }
    else {
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
