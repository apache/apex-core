/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Sink;
import java.util.HashMap;
import sun.java2d.xr.MutableInteger;

/**
 * A sink implementation to collect expected test results in a HashMap
 */
public class TestHashSink<T> implements Sink<T>
{
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
        map.put(tuple,val);
      }
      val.setValue(val.getValue()+1);
      map.put(tuple,null);
    }
  }
}
