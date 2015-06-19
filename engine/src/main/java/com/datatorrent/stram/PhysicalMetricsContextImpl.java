/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import com.datatorrent.api.CustomMetric;

/**
 * Holds physical customMetrics of an operators.
 *
 * @author chandni
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
