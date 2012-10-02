/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Tuple
{
  public DataType getType();

  public long getWindowId();

  public int getIntervalMillis();

  public int getBaseSeconds();
}
