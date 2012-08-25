/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data.DataType;

/**
 *
 * @author chetan
 */
public class Tuple
{
  protected long windowId;
  private final DataType type;

  public Tuple(DataType t)
  {
    type = t;
  }
  /**
   * @return the windowId
   */
  public long getWindowId()
  {
    return windowId;
  }

  /**
   * @param windowId the windowId to set
   */
  public void setWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  /**
   * @return the type
   */
  public DataType getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "type = " + type + " window = " + Long.toHexString(windowId);
  }
}
