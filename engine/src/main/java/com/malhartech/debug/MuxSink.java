/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.debug;

import com.malhartech.api.Sink;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
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
    /* mark all the sinks to be deleted as null */
    int found = 0;
    for (int i = s.length; i-- > 0;) {
      for (int j = sinks.length; j-- > 0;) {
        if (s[i] == sinks[j]) {
          sinks[j] = null;
          found++;
          break;
        }
      }
    }

    /* copy over rest of the sinks to a new array */
    Sink<Object>[] newInstance = (Sink<Object>[])Array.newInstance(Sink.class, sinks.length - found);
    int i = 0;
    for (int j = sinks.length; j-- > 0;) {
      if (sinks[j] != null) {
        newInstance[i++] = sinks[j];
      }
    }

    /* now new array is our final list of sinks */
    sinks = newInstance;
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
