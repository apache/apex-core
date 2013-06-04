/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

import com.malhartech.api.Operator;
import com.malhartech.stram.StramUtils;
import com.malhartech.stram.plan.physical.PlanModifier;

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
  public void execute(PlanModifier pm)
  {
    Class<? extends Operator> operClass = StramUtils.classForName(operatorFQCN, Operator.class);
    Operator operator = StramUtils.newInstance(operClass);
    pm.addOperator(operatorName, operator);
  }

}
