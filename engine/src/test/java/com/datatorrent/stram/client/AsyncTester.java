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
package com.datatorrent.stram.client;

// See https://stackoverflow.com/a/2596530
public class AsyncTester
{
  private Thread thread;
  private volatile AssertionError error;

  public AsyncTester(final Runnable runnable)
  {
    thread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          runnable.run();
        } catch (AssertionError e) {
          error = e;
        }
      }
    });
  }

  public AsyncTester start()
  {
    thread.start();
    return this;
  }

  public void test() throws AssertionError, InterruptedException
  {
    thread.join();
    if (error != null) {
      throw error;
    }
  }
}
