/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical.requests;

import com.datatorrent.stram.StramUtils;
import com.datatorrent.stram.plan.physical.PlanModifier;
import com.datatorrent.api.Operator;

/**
 * <p>CreateOperatorRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
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
