/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

import com.datatorrent.tuple.Tuple;
import com.datatorrent.api.Sink;

/**
 * <p>SweepableReservoir interface.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public interface SweepableReservoir extends Reservoir
{
  public Sink<Object> setSink(Sink<Object> sink);

  public Tuple sweep();

  public int getCount(boolean reset);

}
