/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;

import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

/**
 * <p>StramToNodeChangeLoggersRequest class.</p>
 *
 * @since 1.0.4
 */
public class StramToNodeSetPropertyRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private String propertyKey;
  private String propertyValue;

  public StramToNodeSetPropertyRequest()
  {
    requestType = RequestType.CUSTOM;
    cmd = new SetPropertyCommand();
  }

  public String getPropertyKey()
  {
    return propertyKey;
  }

  public void setPropertyKey(String propertyKey)
  {
    this.propertyKey = propertyKey;
  }

  public String getPropertyValue()
  {
    return propertyValue;
  }

  public void setPropertyValue(String propertyValue)
  {
    this.propertyValue = propertyValue;
  }

  private static final long serialVersionUID = 201405271034L;

  private static final Logger logger = LoggerFactory.getLogger(StramToNodeSetPropertyRequest.class);

  private class SetPropertyCommand implements StatsListener.OperatorCommand, Serializable
  {
    @Override
    public void execute(Operator operator, int id, long windowId) throws IOException
    {
      final Map<String, String> properties = Collections.singletonMap(propertyKey, propertyValue);
      logger.info("Setting property {} on operator {}", properties, operator);
      LogicalPlanConfiguration.setOperatorProperties(operator, properties);
    }

    @Override
    public String toString()
    {
      return "Set Property";
    }
  }
}
