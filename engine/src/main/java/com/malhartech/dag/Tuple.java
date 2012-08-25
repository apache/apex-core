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
  private DataType type;
  private final Object object;

  public Tuple(Object object)
  {
    this.object = object;
  }

  public Object getObject()
  {
    return object;
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

  /**
   * @param type the type to set
   */
  public void setType(DataType type)
  {
    this.type = type;
  }

  @Override
  public String toString()
  {
    return "type = " + type + " window = " + Long.toHexString(windowId);
  }
}
