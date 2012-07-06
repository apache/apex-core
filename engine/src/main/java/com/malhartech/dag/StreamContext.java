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
  
  /**
   * @param sink - target node, not required for output adapter
   */
  public StreamContext(Sink sink) {
    this.sink = sink;
  }
  
  SerDe getSerDe() {
    return null; // required for socket connection
  }
  
  Sink getSink() {
    return sink;
  }
}
