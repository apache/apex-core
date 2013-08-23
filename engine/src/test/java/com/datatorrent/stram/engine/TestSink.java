/**
 * Copyright (c) 2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.api.Sink;
import java.util.ArrayList;

/**
 * A sink implementation to collect expected test results.
 */
public class TestSink implements Sink<Object>
{
  final public ArrayList<Object> collectedTuples = new ArrayList<Object>();

  public void clear()
  {
    this.collectedTuples.clear();
  }

  @Override
  @SuppressWarnings("fallthrough")
  public void put(Object payload)
  {
    synchronized (collectedTuples) {
      collectedTuples.add(payload);
      collectedTuples.notifyAll();
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    return 0;
  }

  public int getResultCount()
  {
    return collectedTuples.size();
  }
//  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
//  {
//    while (collectedTuples.size() < count && timeoutMillis > 0) {
//      timeoutMillis -= 20;
//      synchronized (collectedTuples) {
//        if (collectedTuples.size() < count) {
//          collectedTuples.wait(20);
//        }
//      }
//    }
//  }

}
