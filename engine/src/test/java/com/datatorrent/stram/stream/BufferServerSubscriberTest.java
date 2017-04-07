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
package com.datatorrent.stram.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.engine.SweepableReservoir;

/**
 *
 */
public class BufferServerSubscriberTest
{
  @Test
  public void testEmergencySinks() throws InterruptedException
  {
    final List<Object> list = new ArrayList<>();
    final StreamCodec<Object> myserde = new StreamCodec<Object>()
    {
      @Override
      public Object fromByteArray(Slice fragment)
      {
        if (fragment.offset == 0 && fragment.length == fragment.buffer.length) {
          return fragment.buffer;
        } else {
          return Arrays.copyOfRange(fragment.buffer, fragment.offset, fragment.offset + fragment.length);
        }
      }

      @Override
      public Slice toByteArray(Object o)
      {
        return new Slice((byte[])o, 0, ((byte[])o).length);
      }

      @Override
      public int getPartition(Object o)
      {
        return 0;
      }

    };

    Sink<Object> unbufferedSink = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        list.add(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };

    BufferServerSubscriber bss = new BufferServerSubscriber("subscriber", 5)
    {
      {
        serde = myserde;
      }

      @Override
      public void suspendRead()
      {
        logger.debug("read suspended");
      }

      @Override
      public void resumeRead()
      {
        logger.debug("read resumed");
      }

    };

    SweepableReservoir reservoir = bss.acquireReservoir("unbufferedSink", 3);
    reservoir.setSink(unbufferedSink);

    int i = 0;
    while (i++ < 10) {
      Slice fragment = myserde.toByteArray(new byte[]{(byte)i});
      byte[] buffer = PayloadTuple.getSerializedTuple(myserde.getPartition(i), fragment);
      bss.onMessage(buffer, 0, buffer.length);
    }

    reservoir.sweep(); /* 4 make it to the reservoir */
    reservoir.sweep(); /* we consume the 4; and 4 more make it to the reservoir */
    Assert.assertEquals("4 received", 4, list.size());
    reservoir.sweep(); /* 8 consumed + 2 more make it to the reservoir */
    reservoir.sweep(); /* consume 2 more */
    Assert.assertEquals("10  received", 10, list.size());
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerSubscriberTest.class);
}
