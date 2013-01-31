/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.annotation.ShipContainingJars;
import com.malhartech.api.Sink;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * A sink implementation to collect expected test results in a HashMap
 */
@ShipContainingJars(classes={MutableInt.class})
public class TestArrayListSink<T> implements Sink<T>
{
  public HashMap<Object, MutableInt> map = new HashMap<Object, MutableInt>();
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
    MutableInt val = map.get(key);
    if (val != null) {
      ret = val.intValue();
    }
    return ret;
  }

  @Override
  public void process(T tuple)
  {
    this.count++;
    ArrayList list = (ArrayList) tuple;
    for (Object o: list) {
      MutableInt val = map.get(o);
      if (val == null) {
        val = new MutableInt(0);
        map.put(o, val);
      }
      val.increment();
    }
  }
}
