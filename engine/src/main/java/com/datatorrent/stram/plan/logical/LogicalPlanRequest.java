/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class LogicalPlanRequest
{
  public String getRequestType()
  {
    return this.getClass().getSimpleName();
  }

  public abstract void execute(PlanModifier pm);

}
