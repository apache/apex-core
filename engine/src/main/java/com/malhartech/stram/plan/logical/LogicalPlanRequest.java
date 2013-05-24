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
  private String appId;

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getAppId() {
    return appId;
  }

  public abstract void execute();
}
