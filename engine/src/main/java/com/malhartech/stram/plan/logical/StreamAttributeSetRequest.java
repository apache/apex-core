/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.plan.logical;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class StreamAttributeSetRequest extends LogicalPlanRequest
{
  private String streamName;
  private String attributeName;
  private String attributeValue;

  public String getStreamName()
  {
    return streamName;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  public String getAttributeName()
  {
    return attributeName;
  }

  public void setAttributeName(String attributeName)
  {
    this.attributeName = attributeName;
  }

  public String getAttributeValue()
  {
    return attributeValue;
  }

  public void setAttributeValue(String attributeValue)
  {
    this.attributeValue = attributeValue;
  }

  @Override
  public void execute()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }


}
