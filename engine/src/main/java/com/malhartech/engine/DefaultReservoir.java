/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import com.malhartech.util.CircularBuffer;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DefaultReservoir extends CircularBuffer<Object> implements Reservoir
{
  private Sink<Object> sink;
  private final String id;
  int count;

  public DefaultReservoir(String id, int capacity)
  {
    super(capacity);
    this.id = id;
  }

  @Override
  public void setSink(Sink<Object> sink)
  {
    this.sink = sink;
  }

  @Override
  public Tuple sweep()
  {
    final int size = size();
    for (int i = 1; i <= size; i++) {
      if (peekUnsafe() instanceof Tuple) {
        count += i;
        return (Tuple)peekUnsafe();
      }
      sink.process(pollUnsafe());
    }

    count += size;
    return null;
  }

  @Override
  public String toString()
  {
    return "DefaultReservoir{" + "id=" + id + ", count=" + count + '}';
  }

}
