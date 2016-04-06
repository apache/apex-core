/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.util.ArrayList;

import com.datatorrent.api.Sink;

/**
 * A sink implementation to collect expected test results.
 */
public class TestSink implements Sink<Object>
{
  public final ArrayList<Object> collectedTuples = new ArrayList<>();

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
