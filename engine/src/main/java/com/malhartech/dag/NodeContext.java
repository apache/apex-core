/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer.Data;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeContext implements Context
{
  private Data data;

  public long getCurrentWindowId()
  {
    return data.getWindowId();
  }
  
  public void setData(Data data)
  {
    this.data = data;
  }
  
  public Data getData()
  {
    return data;
  }
}
