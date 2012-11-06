/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Sink;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * A sink implementation to collect expected test results in a HashMap
 */
public class TestArrayListSink<T> implements Sink<T>
{
  public class MutableInteger
  {
    public int value;

    public MutableInteger(int i)
    {
      value = i;
    }
  }
  public HashMap<Object, MutableInteger> map = new HashMap<Object, MutableInteger>();
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
    if (val != null) {
      ret = val.value;
    }
    return ret;
  }

  @Override
  public void process(T tuple)
  {
    this.count++;
    ArrayList list = (ArrayList) tuple;
    for (Object o: list) {
      MutableInteger val = map.get(o);
      if (val == null) {
        val = new MutableInteger(0);
        map.put(o, val);
      }
      val.value++;
    }
  }
}
