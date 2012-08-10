/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ResetWindowTuple extends Tuple
{
  private final int baseSeconds;
  
  public ResetWindowTuple(final int baseSeconds)
  {
    super(null);
    super.setType(Buffer.Data.DataType.RESET_WINDOW);
    
    this.baseSeconds = baseSeconds;
  }
  
  public int getBaseSeconds()
  {
    return baseSeconds;
  }
}
