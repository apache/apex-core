/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 */
package com.datatorrent.stram.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.beanutils.BeanMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;

public class StramToNodeGetPropertyRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private String propertyName;
  private long requestId;

  public StramToNodeGetPropertyRequest()
  {
    requestType = RequestType.CUSTOM;
    cmd = new GetPropertyRequest();
  }

  public long getRequestId()
  {
    return requestId;
  }

  public void setRequestId(long requestId)
  {
    this.requestId = requestId;
  }

  public String getPropertyName()
  {
    return propertyName;
  }

  public void setPropertyName(String propertyName)
  {
    this.propertyName = propertyName;
  }

  private static final Logger logger = LoggerFactory.getLogger(StramToNodeGetPropertyRequest.class);

  private class GetPropertyRequest implements StatsListener.OperatorRequest, Serializable
  {
    @Override
    public StatsListener.OperatorCommandResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      StatsListener.OperatorCommandResponse response = new StatsListener.OperatorCommandResponse();
      BeanMap beanMap = new BeanMap(operator);
      Map<String, Object> propertyValue = new HashMap<String, Object>();
      if (propertyName != null) {
        if (beanMap.containsKey(propertyName)) {
          propertyValue.put(propertyName, beanMap.get(propertyName));
        }
      }
      else {
        Iterator entryIterator = beanMap.entryIterator();
        while (entryIterator.hasNext()) {
          Map.Entry<String, Object> entry = (Map.Entry<String, Object>) entryIterator.next();
          propertyValue.put(entry.getKey(), entry.getValue());
        }
      }
      logger.debug("Getting property {} on operator {}", propertyValue, operator);
      response.requestId = requestId;
      response.object = propertyValue;
      return response;
    }

    @Override
    public String toString()
    {
      return "Get Property";
    }
  }

}
