/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.common.metric.sum;

import java.io.Serializable;
import java.util.Collection;

import com.datatorrent.api.annotation.Name;

import com.datatorrent.common.metric.SingleMetricAggregator;

@Name("sum")
public class DoubleSumAggregator implements SingleMetricAggregator, Serializable
{
  @Override
  public Object aggregate(Collection<Object> metricValues)
  {
    double sum = 0;

    for (Object value : metricValues) {
      sum += ((Number)value).doubleValue();
    }
    return sum;
  }

  private static final long serialVersionUID = 201504081008L;
}
