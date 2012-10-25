/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Sink;
import java.util.ArrayList;
import java.util.List;

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
