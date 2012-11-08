/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.bufferserver.Buffer.Data.DataType;

/**
 *
 * Basic object that is streamed<p>
 * <br>
 * Tuples are of the following type<br>
 * Data:<br>
 * Control: begin window, end window, reset window, end stream<br>
 * heartbeat: To be done, not a high priority<br>
 * <br>
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
