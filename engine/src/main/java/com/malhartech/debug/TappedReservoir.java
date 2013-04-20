/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.debug;

import com.malhartech.api.Sink;
import com.malhartech.engine.SweepableReservoir;
import com.malhartech.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class TappedReservoir implements SweepableReservoir
{
  private final SweepableReservoir reservoir;
  private Sink<Object> originalSink;

  public TappedReservoir(SweepableReservoir reservoir)
  {
    this.reservoir = reservoir;
  }

  @Override
  public void setSink(Sink<Object> sink)
  {
    originalSink = sink;
  }

  @Override
  public Tuple sweep()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int resetCount()
  {
    return reservoir.resetCount();
  }

  @Override
  public int size()
  {
    return reservoir.size();
  }

  @Override
  public Object remove()
  {
    Object object = reservoir.remove();
    return object;
  }

}
