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
public class MuxSink implements Sink<Object>
{
  Sink<Object>[] sinks;
  private int count;

  public MuxSink(Sink<Object>... s)
  {
    sinks = s;
  }

  @SuppressWarnings("unchecked")
  public MuxSink()
  {
    sinks = (Sink<Object>[])Array.newInstance(Sink.class, 0);
  }

  @Override
  public void put(Object tuple)
  {
    count++;
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].put(tuple);
    }
  }

  public void add(Sink<Object>... s)
  {
    int i = sinks.length;
    sinks = Arrays.copyOf(sinks, i + s.length);
    for (Sink<Object> ss: s) {
      sinks[i++] = ss;
    }
  }

  @SuppressWarnings({"unchecked"})
  public void remove(Sink<Object>... s)
  {
    List<Sink<Object>> asList = Arrays.asList(sinks);
    asList.removeAll(Arrays.asList(s));
    sinks = (Sink<Object>[])asList.toArray();
  }

  /**
   * Get the count of sinks supported currently.
   *
   * @return the count of sinks catered.
   */
  public Sink<Object>[] getSinks()
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
