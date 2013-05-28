/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class CreateOperatorRequest extends LogicalPlanRequest
{
  private String operatorName;
  private String operatorFQCN;

  public String getOperatorName()
  {
    return operatorName;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  public String getOperatorFQCN()
  {
    return operatorFQCN;
  }

  public void setOperatorFQCN(String operatorFQCN)
  {
    this.operatorFQCN = operatorFQCN;
  }

  @Override
  public void execute()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
