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
package com.datatorrent.stram.plan.logical;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import com.datatorrent.api.AutoMetric;

/**
 * A class that encapsulates {@link AutoMetric.Aggregator} and {@link AutoMetric.DimensionsScheme} of a particular
 * operator.
 *
 * @since 3.3.0
 */
public final class MetricAggregatorMeta implements Serializable
{
  private final AutoMetric.Aggregator aggregator;
  private final AutoMetric.DimensionsScheme dimensionsScheme;

  protected MetricAggregatorMeta(AutoMetric.Aggregator aggregator, AutoMetric.DimensionsScheme dimensionsScheme)
  {
    this.aggregator = aggregator;
    this.dimensionsScheme = dimensionsScheme;
  }

  public AutoMetric.Aggregator getAggregator()
  {
    return this.aggregator;
  }

  public String[] getDimensionAggregatorsFor(String logicalMetricName)
  {
    if (dimensionsScheme == null) {
      return null;
    }
    return dimensionsScheme.getDimensionAggregationsFor(logicalMetricName);
  }

  public String[] getTimeBuckets()
  {
    if (dimensionsScheme == null) {
      return null;
    }
    return dimensionsScheme.getTimeBuckets();
  }

  private static final long serialVersionUID = 201604271719L;

  /**
   * Serves as a proxy for Aggregator when operator itself implements {@link AutoMetric.Aggregator}.
   */
  static final class MetricsAggregatorProxy implements AutoMetric.Aggregator, Serializable
  {
    private final LogicalPlan.OperatorMeta om;

    MetricsAggregatorProxy(@NotNull LogicalPlan.OperatorMeta om)
    {
      this.om = Preconditions.checkNotNull(om);
    }

    @Override
    public Map<String, Object> aggregate(long windowId, Collection<AutoMetric.PhysicalMetricsContext> physicalMetrics)
    {
      return ((AutoMetric.Aggregator)om.getOperator()).aggregate(windowId, physicalMetrics);
    }

    private static final long serialVersionUID = 201512221830L;
  }
}
