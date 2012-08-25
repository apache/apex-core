/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author chetan
 */
public interface Sink
{
  /**
   *
   * @param t the value of t
   */
  public void process(Object payload);
}
