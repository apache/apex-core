/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.engine.Tuple;

import java.util.HashMap;

/**
 * A sink implementation to collect expected test results in a HashMap
 */
public class TestHashSink<T> implements Sink<T>
{
  public class MutableInteger
  {
    public int value;

    public MutableInteger(int i)
    {
      value = i;
    }
  }
  public HashMap<T, MutableInteger> map = new HashMap<T, MutableInteger>();
  public int count = 0;

  /**
   *
   * @param payload
   */
  public void clear()
  {
    this.map.clear();
    this.count = 0;
  }

  public int getCount(T key)
  {
    int ret = -1;
    MutableInteger val = map.get(key);
    if (val != null)
    {
      ret = val.value;
    }
    return ret;
  }

  @Override
  public void process(T tuple)
  {
    if (tuple instanceof Tuple) {
    }
    else {
      this.count++;
      MutableInteger val = map.get(tuple);
      if (val == null) {
        val = new MutableInteger(0);
        map.put(tuple, val);
      }
      val.value++;
    }
  }
}
