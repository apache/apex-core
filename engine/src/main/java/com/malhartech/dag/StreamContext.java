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
  
  StreamContext(Sink sink) {
    this.sink = sink;
  }
  
  SerDe getSerDe() {
    return null;
  }
  
  Sink getSink() {
    return sink;
  }
}
