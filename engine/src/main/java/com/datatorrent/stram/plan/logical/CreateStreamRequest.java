/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.logical;

import com.datatorrent.stram.plan.physical.PlanModifier;

/**
 * <p>CreateStreamRequest class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.3.2
 */
public class CreateStreamRequest extends LogicalPlanRequest
{
  private String streamName;
  private String sourceOperatorName;
  private String sourceOperatorPortName;
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

  public String getSourceOperatorName()
  {
    return sourceOperatorName;
  }

  public void setSourceOperatorName(String sourceOperatorName)
  {
    this.sourceOperatorName = sourceOperatorName;
  }

  public String getSourceOperatorPortName()
  {
    return sourceOperatorPortName;
  }

  public void setSourceOperatorPortName(String sourceOperatorPortName)
  {
    this.sourceOperatorPortName = sourceOperatorPortName;
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
    pm.addStream(streamName, sourceOperatorName, sourceOperatorPortName, sinkOperatorName, sinkOperatorPortName);
  }

}
