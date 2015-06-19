/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import java.lang.annotation.*;
import java.util.Collection;
import java.util.Map;

/**
 * A field which tracks an aspect of operator's progress is considered a metric. The value
 * of the field is communicated from the execution environment to application master. The declared field
 * name will be used as the metric key.
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomMetric
{
  /**
   * Represents collection of physical metrics.
   */
  public static interface PhysicalMetricsContext
  {
    /**
     * @return map of metric name to value
     */
     Map<String, Object> getCustomMetrics();

    /**
     *
     * @return operator id
     */
     int operatorId();
  }

  /**
   * It aggregates custom metrics from multiple physical partitions of an operator to a logical one.<br/>
   * An aggregator is provided as operator attribute. By default when there isn't any aggregator set the engine does
   * summation of a number custom metric.
   */
  public static interface Aggregator
  {
    /**
     * Aggregates values of a specific metric.
     *
     * @param windowId        window id for which the values are aggregated.
     * @param physicalMetrics collection of physical metrics from all the instances.
     * @return map of aggregated metric keys and values.
     */
    Map<String, Object> aggregate(long windowId, Collection<PhysicalMetricsContext> physicalMetrics);
  }
}
