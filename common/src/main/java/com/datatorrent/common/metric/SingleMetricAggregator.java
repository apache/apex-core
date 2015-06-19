/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.common.metric;

import java.util.Collection;

public interface SingleMetricAggregator
{
  Object aggregate(Collection<Object> metricValues);
}
