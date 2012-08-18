/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import java.io.Serializable;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class HeartbeatCounters implements Serializable
{
  private static final long serialVersionUID = 201208171717L;
  public int tuplesProcessed;
  public long bytesProcessed;
  public long windowId;
}
