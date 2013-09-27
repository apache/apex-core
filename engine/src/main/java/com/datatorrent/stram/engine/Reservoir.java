/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

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
   * Remove an element from the reservoir.
   *
   * @return the removed element.
   */
  public Object remove();

}
