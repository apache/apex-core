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
   * To facilitate optimal operation of the DAG components, some Sinks may choose to replace
   * themselves with optimal versions of themselves. When they wish to do so, they will have
   * this method throw MutatedSinkException instead of processing the tuple. The caller will
   * have to call process again on the new sink passed through the exception.
   *
   * To make sure that this does not result in try catch blocks throughout the code, the
   * caller may assume that this exception will not be thrown for the payload which are of
   * type other than the one meant for representing beginning of the window.
   *
   * @param tuple payload to be processed by this sink.
   */
  public void process(T tuple);
}
