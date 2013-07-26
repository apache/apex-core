/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.debug;

import com.datatorrent.engine.SweepableReservoir;
import com.datatorrent.tuple.Tuple;
import com.datatorrent.api.Sink;

/**
 * <p>TappedReservoir class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class TappedReservoir extends MuxSink implements SweepableReservoir
{
  public final SweepableReservoir reservoir;
  private Sink<Object> sink;

  @SuppressWarnings({"unchecked", "LeakingThisInConstructor"})
  public TappedReservoir(SweepableReservoir reservoir, Sink<Object> tap)
  {
    this.reservoir = reservoir;
    add(tap);
    sink = reservoir.setSink(this);
  }

  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    try {
      return this.sink;
    }
    finally {
      this.sink = sink;
    }
  }

  @Override
  public Tuple sweep()
  {
    return reservoir.sweep();
  }

  @Override
  public int getCount(boolean reset)
  {
    return reservoir.getCount(reset);
  }

  @Override
  public int size()
  {
    return reservoir.size();
  }

  @Override
  public void put(Object tuple)
  {
    super.put(tuple);
    sink.put(tuple);
  }

  @Override
  public Object remove()
  {
    Object object = reservoir.remove();
    super.put(object);
    return object;
  }

}
