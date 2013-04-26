/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface SweepableReservoir extends Reservoir
{
  public void setSink(Sink<Object> sink);

  public Tuple sweep();

  public int resetCount();

}
