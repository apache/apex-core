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
    cmd = new SetPropertyRequest();
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

  private class SetPropertyRequest implements StatsListener.OperatorRequest, Serializable
  {
    @Override
    public StatsListener.OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
    {
      final Map<String, String> properties = Collections.singletonMap(propertyKey, propertyValue);
      logger.info("Setting property {} on operator {}", properties, operator);
      LogicalPlanConfiguration.setOperatorProperties(operator, properties);
      return null;
    }

    @Override
    public String toString()
    {
      return "Set Property";
    }
  }
}
