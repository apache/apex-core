/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical.requests;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 * <p>RemoveOperatorRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
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
