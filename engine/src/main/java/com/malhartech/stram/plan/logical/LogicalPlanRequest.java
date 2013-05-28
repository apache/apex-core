/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public abstract class LogicalPlanRequest
{
  public String getRequestType() {
    return this.getClass().getName();
  }
  
  public abstract void execute();
}
