/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */

package com.datatorrent.stram.plan.logical;

import java.io.Serializable;

/**
 *
 * @author David Yan <david@datatorrent.com>
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
}
