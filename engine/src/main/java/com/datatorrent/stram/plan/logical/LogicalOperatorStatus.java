/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.plan.logical;

import com.datatorrent.api.Context.Counters;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>LogicalOperatorStatus class.</p>
 *
 * @author David Yan <david@datatorrent.com>
 * @since 0.9.5
 */
public class LogicalOperatorStatus implements Serializable
{
  private static final long serialVersionUID = 201404251445L;

  public LogicalOperatorStatus(String name)
  {
    this.name = name;
  }
  public final String name;
  public long totalTuplesProcessed = 0;
  public long totalTuplesEmitted = 0;
  public long failureCount = 0;
  public List<Counters> counters = new ArrayList<Counters>();
}
