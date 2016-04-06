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
package com.datatorrent.stram.debug;

import org.junit.Assert;

import org.junit.Test;

import com.datatorrent.api.Sink;

/**
 *
 */
public class MuxSinkTest
{
  public MuxSinkTest()
  {
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testAdd()
  {
    Sink<Object> a = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };

    Sink<Object> b = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };

    MuxSink instance1 = new MuxSink(a, b);
    Assert.assertEquals("2 sinks", instance1.getSinks().length, 2);

    MuxSink instance2 = new MuxSink();
    instance2.add(a, b);
    Assert.assertEquals("2 sinks", instance2.getSinks().length, 2);
  }

}
