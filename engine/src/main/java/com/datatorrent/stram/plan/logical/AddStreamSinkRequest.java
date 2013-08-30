/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 * <p>AddStreamRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.5
 */
public class AddStreamSinkRequest extends LogicalPlanRequest
{
  private String streamName;
  private String sinkOperatorName;
  private String sinkOperatorPortName;

  public String getStreamName()
  {
    return streamName;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  public String getSinkOperatorName()
  {
    return sinkOperatorName;
  }

  public void setSinkOperatorName(String sinkOperatorName)
  {
    this.sinkOperatorName = sinkOperatorName;
  }

  public String getSinkOperatorPortName()
  {
    return sinkOperatorPortName;
  }

  public void setSinkOperatorPortName(String sinkOperatorPortName)
  {
    this.sinkOperatorPortName = sinkOperatorPortName;
  }

  @Override
  public void execute(PlanModifier pm)
  {
    pm.addSink(streamName, sinkOperatorName, sinkOperatorPortName);
  }

}
