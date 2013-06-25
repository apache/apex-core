/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 *
 * @author David Yan <david@datatorrent.com>
 */
public class RemoveOperatorRequest extends LogicalPlanRequest
{
  private String operatorName;

  public String getOperatorName()
  {
    return operatorName;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  @Override
  public void execute(PlanModifier pm)
  {
    pm.removeOperator(operatorName);
  }

}
