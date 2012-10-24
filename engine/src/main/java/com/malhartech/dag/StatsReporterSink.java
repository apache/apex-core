/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Sink;
import com.malhartech.api.Stats.StatsReporter;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface StatsReporterSink<T> extends Sink<T>, StatsReporter
{
  public static final StatsReporterSink<?>[] NO_SINKS = new StatsReporterSink[0];
}
