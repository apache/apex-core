/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface Reservoir
{
  /**
   * the count of elements in this SweepableReservoir.
   *
   * @return the count
   */
  public int size();

  /**
   * Remove the element from head/tail?
   */
  public Object remove();

}
