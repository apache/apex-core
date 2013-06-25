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
public class RemoveStreamRequest extends LogicalPlanRequest
{
  private String streamName;

  public String getStreamName()
  {
    return streamName;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  @Override
  public void execute(PlanModifier pm)
  {
    pm.removeStream(streamName);
  }

}
