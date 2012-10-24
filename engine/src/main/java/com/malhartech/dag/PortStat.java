/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Stats;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class PortStats implements Stats
{
  final String portname;
  final int processedCount;

  PortStats(String name, int count)
  {
    portname = name;
    processedCount= count;
  }
}
