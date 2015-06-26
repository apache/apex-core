/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.datatorrent.api.CustomMetric;

/**
 * Holds physical customMetrics of an operators.
 *
 */
public class PhysicalMetricsContextImpl implements CustomMetric.PhysicalMetricsContext
{
  private final int operatorId;
  private final Map<String, Object> customMetrics;

  PhysicalMetricsContextImpl(int operatorId, Map<String, Object> customMetrics)
  {
    this.operatorId = operatorId;
    this.customMetrics = customMetrics;
  }

  @Override
  public Map<String, Object> getCustomMetrics()
  {
    return customMetrics;
  }

  @Override
  public int operatorId()
  {
    return operatorId;
  }
}
