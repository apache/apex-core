/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

/**
 * <p>Reservoir interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
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
