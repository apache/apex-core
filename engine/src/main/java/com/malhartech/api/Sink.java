/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 * Abstraction for the processing logic or consumption of a data tuple.
 * Implemented by concrete data ports for their processing behavior or by streams.
 */
public interface Sink<T extends Object>
{
  public static final Sink<?>[] NO_SINKS = new Sink[0];

  /**
   * Process the payload which can either be user defined object or Tuple.
   *
   * @param tuple payload to be processed by this sink.
   */
  public void process(T tuple);
}
