/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;

/**
 *
 * @author chetan
 */
public class Tuple
{

  private StreamContext context;
  private Data data;
  final Object object;

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

  /**
   * @return the data
   */
  public Data getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(Data data)
  {
    this.data = data;
  }

  public Object getObject()
  {
    return object;
  }
}
