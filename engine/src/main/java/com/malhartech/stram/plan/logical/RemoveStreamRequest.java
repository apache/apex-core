/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
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
  public void execute()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
