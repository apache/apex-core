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
package com.datatorrent.api;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Map;

/**
 * A field which tracks an aspect of operator's progress is considered a metric. The value
 * of the field is communicated from the execution environment to application master. The declared field
 * name will be used as the metric key.
 *
 * @since 3.0.0
 */
@Documented
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoMetric
{
  /**
   * Represents collection of physical metrics.
   */
  interface PhysicalMetricsContext
  {
    /**
     * @return map of metric name to value
     */
    Map<String, Object> getMetrics();

    /**
     * @return operator id
     */
    int operatorId();
  }

  /**
   * It aggregates metrics from multiple physical partitions of an operator to a logical one.<br/>
   * An aggregator is provided as operator attribute. By default, when there isn't any aggregator set explicitly,
   * the application master sums up all the number metrics.
   */
  interface Aggregator
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

  /**
   * Provides information of dimension aggregations and time-buckets which are sent to Application data tracker.<br/>
   * Application data tracker by default does certain aggregations for 1m, 1h,& 1d time buckets unless it overridden by
   * the app developer by providing a dimension scheme as operator attribute.
   */
  interface DimensionsScheme
  {
    /**
     * Time buckets for eg. {1m, 1h}. Application data tracker by default does 1m, 1h & 1d aggregations but this
     * will override it to just perform 1m and 1h aggregations. <br/>
     * <p/>
     * Time bucket format is a number followed by one of the below characters:
     * <ul>
     * <li>s - second</li>
     * <li>m - minute</li>
     * <li>h - hour</li>
     * <li>d - day</li>
     * <li>w - week</li>
     * <li>M - month</li>
     * <li>q - quarter</li>
     * <li>y - year</li>
     * </ul>
     *
     * @return time buckets.
     */
    String[] getTimeBuckets();

    /**
     * Application data tracker by default performs SUM, MIN, MAX, AVG, COUNT, FIRST, LAST on all number metrics.
     * An app developer can influence this behavior by creating a dimension scheme that has a mapping of logical metric name
     * to aggregations. Stram will invoke this method for each logical metric and check if the aggregations are overwritten
     * and will inform that to app data tracker.
     *
     * @param logicalMetricName logical metric name.
     * @return aggregations eg. SUM, MIN, MAX, etc. that will be performed by app data tracker on a logical metric.
     */
    String[] getDimensionAggregationsFor(String logicalMetricName);
  }

  /**
   * Interface of transport for STRAM to push metrics data
   */
  interface Transport
  {
    /**
     * Pushes the metrics data (in JSON) to the transport.
     *
     * @param jsonData The metric data in JSON to be pushed to this transport
     */
    void push(String jsonData) throws IOException;

    /**
     * Returns the number of milliseconds for resending the metric schema. The schema will need to be resent for
     * unreliable transport. Return 0 if the schema does not need to be resent.
     */
    long getSchemaResendInterval();
  }
}
