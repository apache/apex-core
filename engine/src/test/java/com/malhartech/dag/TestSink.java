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
public class TestSink<T> implements Sink
{
  final public List<T> collectedTuples = new ArrayList<T>();

  /**
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    if (payload instanceof Tuple) {
    }
    else {
      synchronized (collectedTuples) {
        collectedTuples.add((T)payload);
        collectedTuples.notifyAll();
      }
    }
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException {
    synchronized (collectedTuples) {
      if (collectedTuples.size() < count) {
        collectedTuples.wait(timeoutMillis);
      }
    }
  }


}
