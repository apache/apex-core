/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

/**
 * A sink implementation to collect expected test results.
 */
public class TestCountSink<T> extends TestSink<T>
{
  public  int count = 0;

  @Override
  public void clear()
  {
    count = 0;
    super.clear();
  }

  public int getCount()
  {
    return count;
  }

  /**
   *
   * @param payload
   */
  @Override
  public void process(T payload)
  {
    if (payload instanceof Tuple) {
    }
    else {
      count++;
    }
  }
}
