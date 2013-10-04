/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.tuple.Tuple;

/**
 * A non buffering stream which facilitates the ThreadLocal implementation of an operator.
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.5
 */
public class OiOStream implements Stream, SweepableReservoir
{
  private Sink<Object> sink;
  private Sink<Tuple> control;
  private int count;

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setControlSink(Sink<Tuple> control)
  {
    this.control = control;
  }
  
  @Override
  public void activate(StreamContext cntxt)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void put(Object t)
  {
    if (t instanceof Tuple) {
      control.put((Tuple)t);
    }
    else {
      count++;
      sink.put(t);
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    }
    finally {
      if (reset) {
        count = 0;
      }
    }
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  /**
   * OiOStream is active when there is exactly one tuple present.
   * It's an error to have more than one tuple active on OiO.
   */
  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public Object remove()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
