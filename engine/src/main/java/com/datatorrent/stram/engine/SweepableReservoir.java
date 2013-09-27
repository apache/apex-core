/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.stram.tuple.Tuple;
import com.datatorrent.api.Sink;

/**
 * <p>SweepableReservoir interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public interface SweepableReservoir extends Reservoir
{
  /**
   * Set a new sink on this reservoir where data tuples would be put.
   *
   * @param sink The new Sink for the data tuples
   * @return The old sink if present or null
   */
  public Sink<Object> setSink(Sink<Object> sink);

  /**
   * Consume all the data tuples until control tuple is encountered.
   *
   * @return The control tuple encountered or null
   */
  public Tuple sweep();

  /**
   * Get the count of tuples consumed.
   *
   * @param reset flag to indicate if the count should be reset to zero after this operation
   * @return the count of tuples
   */
  public int getCount(boolean reset);

}
