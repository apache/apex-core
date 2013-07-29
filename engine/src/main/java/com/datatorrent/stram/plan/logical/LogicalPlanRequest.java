/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 * <p>Abstract LogicalPlanRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public abstract class LogicalPlanRequest
{
  public String getRequestType()
  {
    return this.getClass().getSimpleName();
  }

  public abstract void execute(PlanModifier pm);

}
