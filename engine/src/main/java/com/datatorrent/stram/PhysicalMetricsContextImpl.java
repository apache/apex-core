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
package com.datatorrent.stram;

import java.util.Map;

import com.datatorrent.api.AutoMetric;

/**
 * Holds physical metrics of an operators.
 *
 * @since 3.0.0
 */
public class PhysicalMetricsContextImpl implements AutoMetric.PhysicalMetricsContext
{
  private final int operatorId;
  private final Map<String, Object> metrics;

  PhysicalMetricsContextImpl(int operatorId, Map<String, Object> metrics)
  {
    this.operatorId = operatorId;
    this.metrics = metrics;
  }

  @Override
  public Map<String, Object> getMetrics()
  {
    return metrics;
  }

  @Override
  public int operatorId()
  {
    return operatorId;
  }
}
