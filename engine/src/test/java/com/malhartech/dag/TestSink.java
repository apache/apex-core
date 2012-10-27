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
public class TestSink<T> implements Sink<T>
{
  final public List<T> collectedTuples = new ArrayList<T>();

  /**
   *
   * @param payload
   */
  public void clear()
  {
    this.collectedTuples.clear();
  }

  @Override
  public void process(T payload)
  {
    if (payload instanceof Tuple) {
    }
    else {
      synchronized (collectedTuples) {
        collectedTuples.add(payload);
        collectedTuples.notifyAll();
      }
    }
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < count && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < count) {
          collectedTuples.wait(20);
        }
      }
    }
  }
}
