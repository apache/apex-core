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
package com.datatorrent.api;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultOutputPortTest
{
  private static final Logger logger = LoggerFactory.getLogger(DefaultOutputPortTest.class);

  private DefaultOutputPort<Object> port;
  private Sink<Object> sink;

  @Before
  public void setupTest()
  {
    port = new DefaultOutputPort<>();
    sink = new Sink<Object>()
    {
      private volatile int count = 0;

      @Override
      public void put(Object tuple)
      {
        count++;
      }

      @Override
      public int getCount(boolean reset)
      {
        return count;
      }
    };
    port.setSink(sink);
  }

  /*
   * Same thread for setup() and emit()
   */
  @Test
  public void testSameThreadForSetupAndEmit()
  {
    port.setup(null);
    port.emit(null);
    Assert.assertEquals(1, sink.getCount(false));
    // if it comes here it passes
  }

  /*
   * setup() not called : null thread object should not cause exception
   */
  @Test
  public void testSetupNotCalledAndEmit()
  {
    port.emit(null);
    Assert.assertEquals(1, sink.getCount(false));
    // if it comes here it passes
  }

  volatile boolean pass = false;

  /*
   * Different thread for setup() and emit()
   */
  @Test
  public void testDifferentThreadForSetupAndEmit() throws InterruptedException
  {
    System.clearProperty(DefaultOutputPort.THREAD_AFFINITY_DISABLE_CHECK);  // do not suppress the check
    pass = false;
    port.setup(null);
    Thread thread = new Thread("test-thread-xyz")
    {
      @Override
      public void run()
      {
        try {
          port.emit(null);
        } catch (IllegalStateException ise) {
          pass = ise.getMessage().startsWith("Current thread test-thread-xyz is different from the operator thread ");
        }
      }
    };
    thread.start();
    thread.join();
    Assert.assertTrue("same thread check didn't take place!", pass);
    Assert.assertEquals(0, sink.getCount(false)); // no put() on sink
  }

  /*
   * Different thread for setup() and emit() but suppress check property set
   */
  @Test
  public void testDifferentThreadForSetupAndEmit_CheckSuppressed() throws InterruptedException
  {
    System.setProperty(DefaultOutputPort.THREAD_AFFINITY_DISABLE_CHECK, "true");  // suppress the check
    port.setup(null);
    pass = true;
    Thread thread = new Thread()
    {
      @Override
      public void run()
      {
        try {
          port.emit(null);
        } catch (IllegalStateException ise) {
          pass = false;
        }
      }
    };
    thread.start();
    thread.join();
    Assert.assertEquals("same thread check was not suppressed!", 1, sink.getCount(false));
    Assert.assertTrue("Exception was thrown!", pass);
    System.clearProperty(DefaultOutputPort.THREAD_AFFINITY_DISABLE_CHECK);
  }
}
