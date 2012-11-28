/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.engine;

/**
 * A sink implementation to collect expected test results.
 */
public class TestCountAndLastTupleSink<T> extends TestCountSink<T>
{
  public  Object tuple = null;
  /**
   *
   * @param payload
   */

  @Override
  public void clear()
  {
    this.tuple = null;
    super.clear();
  }

  @Override
  public void process(T tuple)
  {
    if (tuple instanceof Tuple) {

    }
    else {
      this.tuple = tuple;
      count++;
    }
  }
}
