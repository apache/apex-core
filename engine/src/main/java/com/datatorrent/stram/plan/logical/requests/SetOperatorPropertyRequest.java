/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical.requests;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 * <p>SetOperatorPropertyRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class SetOperatorPropertyRequest extends LogicalPlanRequest
{
  private String operatorName;
  private String propertyName;
  private String propertyValue;

  public String getOperatorName()
  {
    return operatorName;
  }

  public void setOperatorName(String operatorName)
  {
    this.operatorName = operatorName;
  }

  public String getPropertyName()
  {
    return propertyName;
  }

  public void setPropertyName(String propertyName)
  {
    this.propertyName = propertyName;
  }

  public String getPropertyValue()
  {
    return propertyValue;
  }

  public void setPropertyValue(String propertyValue)
  {
    this.propertyValue = propertyValue;
  }

  @Override
  public void execute(PlanModifier pm)
  {
    pm.setOperatorProperty(operatorName, propertyName, propertyValue);
  }

}
