/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.beanutils.BeanMap;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;
import com.datatorrent.stram.engine.OperatorResponse;

/**
 * <p>StramToNodeGetPropertyRequest class.</p>
 *
 * @since 2.1.0
 */
public class StramToNodeGetPropertyRequest extends StreamingContainerUmbilicalProtocol.StramToNodeRequest implements Serializable
{
  private String propertyName;

  public StramToNodeGetPropertyRequest()
  {
    requestType = RequestType.CUSTOM;
    cmd = new GetPropertyRequest();
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
    public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      BeanMap beanMap = new BeanMap(operator);
      Map<String, Object> propertyValue = new HashMap<>();
      if (propertyName != null) {
        if (beanMap.containsKey(propertyName)) {
          propertyValue.put(propertyName, beanMap.get(propertyName));
        }
      } else {
        Iterator entryIterator = beanMap.entryIterator();
        while (entryIterator.hasNext()) {
          Map.Entry<String, Object> entry = (Map.Entry<String, Object>)entryIterator.next();
          propertyValue.put(entry.getKey(), entry.getValue());
        }
      }
      logger.debug("Getting property {} on operator {}", propertyValue, operator);
      OperatorResponse response = new OperatorResponse(requestId, propertyValue);
      return response;
    }

    @Override
    public String toString()
    {
      return "Get Property";
    }
  }

}
