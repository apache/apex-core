/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

import com.malhartech.api.BaseOperator;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class CreateOperatorRequest extends LogicalPlanRequest
{
  private String operatorName;
  private Class<BaseOperator> operatorClass;

  public String getOperatorName()
  {
    return operatorName;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  public Class<BaseOperator> getOperatorClass()
  {
    return operatorClass;
  }

  public void setOperatorClass(Class<BaseOperator> operatorClass)
  {
    this.operatorClass = operatorClass;
  }

  @Override
  public void execute()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
