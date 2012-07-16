/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 * Defines the destination for tuples processed.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StreamContext implements Context
{
  private Sink sink;
  private SerDe serde;
  private long windowId;
  
  /**
   * @param sink - target node, not required for output adapter
   */
  public void setSink(Sink sink) {
    this.sink = sink;
  }
  
  public SerDe getSerDe() {
    return serde; // required for socket connection
  }

  public void setSerde(SerDe serde) {
    this.serde = serde;
  }

  public Sink getSink() {
    return sink;
  }

  public long getWindowId()
  {
    return windowId;
  }
  
  public void setWindowId(long windowId)
  {
    this.windowId = windowId;
  }
}
