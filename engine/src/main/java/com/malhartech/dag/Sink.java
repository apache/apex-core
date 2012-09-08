/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * TBD<p>
 * <br>
 *
 * @author chetan
 */
public interface Sink
{
  public static final Sink[] NO_SINKS = new Sink[0];

  /**
   * Process the payload which can either be used defined object or Tuple.
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
   * @param payload payload to be processed by this sink.
   */
  public void process(Object payload);
}
