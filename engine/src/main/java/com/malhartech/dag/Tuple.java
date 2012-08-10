/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data.DataType;

/**
 *
 * @author chetan
 */
public class Tuple
{
  private int windowId;
  private DataType type;
  private StreamContext context;
  private final Object object;

  public Tuple(Object object)
  {
    this.object = object;
  }

  /**
   * @return the context
   */
  public StreamContext getContext()
  {
    return context;
  }

  /**
   * @param context the context to set
   */
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  public Object getObject()
  {
    return object;
  }

  /**
   * @return the windowId
   */
  public int getWindowId()
  {
    return windowId;
  }

  /**
   * @param windowId the windowId to set
   */
  public void setWindowId(int windowId)
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
    return "type = " + type + " window = " + windowId;
  }
}
