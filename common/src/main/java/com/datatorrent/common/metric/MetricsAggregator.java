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
package com.datatorrent.common.metric;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.annotation.Name;

/**
 * An easy to use {@link AutoMetric.Aggregator} that can be configured to perform multiple aggregations on each
 * {@link AutoMetric} in an operator.
 * <p/>
 * The user needs to add an array of {@link SingleMetricAggregator}s for each metric. All the aggregators on each
 * metric will be executed during aggregation.
 * <p/>
 * There are examples of {@link SingleMetricAggregator} provided in the library for common number
 * aggregations- sum, min, max, avg.
 *
 * @since 3.0.0
 */
public class MetricsAggregator implements AutoMetric.Aggregator, Serializable
{
  protected static final String DEFAULT_SEPARATOR = "-";

  //physical metric -> collection of logical metrics
  protected final Map<String, List<LogicalMetricMeta>> metricLogicalAggregates;
  protected String aggregatorMetricSeparator;

  public MetricsAggregator()
  {
    metricLogicalAggregates = Maps.newHashMap();
    aggregatorMetricSeparator = DEFAULT_SEPARATOR;
  }

  @Override
  public Map<String, Object> aggregate(long windowId, Collection<AutoMetric.PhysicalMetricsContext> physicalMetrics)
  {
    Multimap<String, Object> metricValues = ArrayListMultimap.create();

    for (AutoMetric.PhysicalMetricsContext pmCtx : physicalMetrics) {
      for (Map.Entry<String, Object> entry : pmCtx.getMetrics().entrySet()) {
        metricValues.put(entry.getKey(), entry.getValue());
      }
    }

    Map<String, Object> aggregates = Maps.newHashMap();
    for (String metric : metricValues.keySet()) {
      List<LogicalMetricMeta> logicalMetricMetas = metricLogicalAggregates.get(metric);
      if (logicalMetricMetas != null) {
        for (LogicalMetricMeta logicalMetricMeta : logicalMetricMetas) {

          Object aggregatedVal = logicalMetricMeta.aggregator.aggregate(metricValues.get(metric));
          aggregates.put(logicalMetricMeta.name, aggregatedVal);
        }
      }
    }
    return aggregates;
  }

  /**
   * This can be overridden to change logical metric name.
   *
   * @param metric     metric name
   * @param aggregator aggregator
   * @return logical metric name which is assigned to the result of the aggregator.
   */
  protected String deriveLogicalMetricName(String metric, SingleMetricAggregator aggregator)
  {
    Name aggregatorName = aggregator.getClass().getAnnotation(Name.class);
    String aggregatorDesc;
    if (aggregatorName == null) {
      aggregatorDesc = aggregator.getClass().getName();
    } else {
      aggregatorDesc = aggregatorName.value();
    }
    return aggregatorDesc + aggregatorMetricSeparator + metric;
  }

  /**
   * Adds aggregators for a single physical metric.<br/>
   * If there is only one aggregator, then the logical metric name is same as physical metric name.<br/>
   * Otherwise logical metric names are derived using {@link #deriveLogicalMetricName}
   *
   * @param metric      physical metric name.
   * @param aggregators aggregators for the metric.
   */
  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    addAggregatorsHelper(metric, aggregators, null);
  }

  /**
   * Adds aggregators and logical metric names for a single physical metric.
   *
   * @param metric             physical metric name.
   * @param aggregators        aggregators for the metric.
   * @param logicalMetricNames logic metric names that will be used for an aggregated value. logicalMetricNames[i] will
   *                           be used for the result of aggregators[i].
   */
  public void addAggregators(@NotNull String metric, @NotNull SingleMetricAggregator[] aggregators,
      @NotNull String[] logicalMetricNames)
  {
    Preconditions.checkNotNull(metric, "metric");
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkNotNull(logicalMetricNames, "logicalMetricNames");
    Preconditions.checkArgument(aggregators.length == logicalMetricNames.length,
        "different length aggregators and logical names");
    addAggregatorsHelper(metric, aggregators, logicalMetricNames);
  }

  private void addAggregatorsHelper(String metric, SingleMetricAggregator[] aggregators, String[] logicalMetricNames)
  {
    List<LogicalMetricMeta> laggregators = metricLogicalAggregates.get(metric);
    if (laggregators == null) {
      laggregators = Lists.newArrayList();
      metricLogicalAggregates.put(metric, laggregators);
    }
    for (int i = 0; i < aggregators.length; i++) {

      String resultName = (logicalMetricNames == null || logicalMetricNames[i] == null) ?
          (aggregators.length == 1 ? metric : deriveLogicalMetricName(metric, aggregators[i])) : logicalMetricNames[i];

      laggregators.add(new LogicalMetricMeta(aggregators[i], resultName));
    }
  }

  public String getAggregatorMetricSeparator()
  {
    return aggregatorMetricSeparator;
  }

  public void setAggregatorMetricSeparator(String aggregatorMetricSeparator)
  {
    this.aggregatorMetricSeparator = aggregatorMetricSeparator;
  }

  public static class LogicalMetricMeta implements Serializable
  {
    private SingleMetricAggregator aggregator;

    private String name;

    protected LogicalMetricMeta(@NotNull SingleMetricAggregator aggregator, @NotNull String name)
    {
      this.aggregator = Preconditions.checkNotNull(aggregator, "aggregator");
      this.name = Preconditions.checkNotNull(name, "logical metric name");
    }

    public SingleMetricAggregator getAggregator()
    {
      return aggregator;
    }

    protected void setAggregator(SingleMetricAggregator aggregator)
    {
      this.aggregator = aggregator;
    }

    public String getName()
    {
      return name;
    }

    protected void setName(String name)
    {
      this.name = name;
    }

    private static final long serialVersionUID = 201604231340L;
  }

  private static final long serialVersionUID = 201604231337L;
}
