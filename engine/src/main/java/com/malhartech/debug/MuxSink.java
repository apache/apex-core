/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.debug;

import com.malhartech.api.Sink;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class MuxSink<T> implements Sink<T>
{
  Sink<T>[] sinks;
  private int count;

  public MuxSink(Sink<T>... s)
  {
    sinks = s;
  }

  @SuppressWarnings("unchecked")
  public MuxSink()
  {
    sinks = (Sink<T>[])Array.newInstance(Sink.class, 0);
  }

  @Override
  public void put(T tuple)
  {
    count++;
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].put(tuple);
    }
  }

  public void add(Sink<T>... s)
  {
    int i = sinks.length;
    sinks = Arrays.copyOf(sinks, i + s.length);
    for (Sink<T> ss: s) {
      sinks[i++] = ss;
    }
  }

  @SuppressWarnings({"unchecked"})
  public void remove(Sink<Object>... s)
  {
    List<Sink<T>> asList = Arrays.asList(sinks);
    asList.removeAll(Arrays.asList(s));
    sinks = (Sink<T>[])asList.toArray();
  }

  /**
   * Get the count of sinks supported currently.
   *
   * @return the count of sinks catered.
   */
  public Sink<T>[] getSinks()
  {
    return Arrays.copyOf(sinks, sinks.length);
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

}
