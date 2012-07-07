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
  final Sink sink;
  private SerDe serde;
  
  /**
   * @param sink - target node, not required for output adapter
   */
  public StreamContext(Sink sink) {
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
}
