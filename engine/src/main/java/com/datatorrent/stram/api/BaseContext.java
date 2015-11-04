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

import java.util.Collection;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;

import com.datatorrent.stram.util.AbstractWritableAdapter;

/**
 * <p>BaseContext class.</p>
 *
 * @since 0.3.2
 */
public class BaseContext extends AbstractWritableAdapter implements Context
{
  /*
   * the following 2 need to be public since otherwise they are not serialized.
   */
  public final AttributeMap attributes;
  public final Context parentContext; // may be we do not need to serialize parentContext!
  @Deprecated
  public Object counters;
  public Collection<String> metricsToSend;
  private boolean metricsListed;

  public BaseContext(AttributeMap attributes, Context parentContext)
  {
    this.attributes = attributes == null ? new DefaultAttributeMap() : attributes;
    this.parentContext = parentContext;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return attributes;
  }

  @Override
  public <T> T getValue(Attribute<T> key)
  {
    T attr = attributes.get(key);
    if (attr != null) {
      return attr;
    }
    return parentContext == null ? key.defaultValue : parentContext.getValue(key);
  }

  @Override
  public void setCounters(Object counters)
  {
    this.counters = counters;
  }

  @Override
  public void sendMetrics(Collection<String> metricNames)
  {
    metricsListed = true;
    metricsToSend = metricNames;
  }

  public boolean areMetricsListed()
  {
    return metricsListed;
  }

  public void clearMetrics()
  {
    metricsToSend = null;
    metricsListed = false;
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  private static final long serialVersionUID = 201306060103L;
}
