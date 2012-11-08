/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import java.io.Serializable;

/**
 *
 * Data for heartbeat from node to stram<p>
 * <br>
 * Basic data sent by operators to stram during heartbeat<br>
 * <br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class HeartbeatCounters implements Serializable
{
  private static final long serialVersionUID = 201208171717L;
  public int tuplesProcessed;
  public long tuplesProduced;
  public long windowId;
}
