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
package com.datatorrent.bufferserver.packet;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import static com.datatorrent.bufferserver.packet.SubscribeRequestTuple.getSerializedRequest;

/**
 *
 */
public class SubscribeRequestTupleTest
{
  public SubscribeRequestTupleTest()
  {
  }

  @Test
  public void testGetSerializedRequest()
  {
    String id = "SubscriberId";
    String down_type = "SubscriberId/StreamType";
    String upstream_id = "PublisherId";
    int mask = 7;
    ArrayList<Integer> partitions = new ArrayList<Integer>();
    partitions.add(5);
    long startingWindowId = 0xcafebabe00000078L;

    byte[] serial = getSerializedRequest(null, id, down_type, upstream_id, mask, partitions, startingWindowId, 32 * 1024);
    SubscribeRequestTuple tuple = (SubscribeRequestTuple)Tuple.getTuple(serial, 0, serial.length);

    Assert.assertEquals("identifier", tuple.getIdentifier(), id);
    Assert.assertEquals("UpstreamType", tuple.getStreamType(), down_type);
    Assert.assertEquals("UpstreamId", tuple.getUpstreamIdentifier(), upstream_id);
    Assert.assertEquals("mask", tuple.getMask(), mask);
    Assert.assertEquals("BufferSize", tuple.getBufferSize(), 32 * 1024);
    int[] parts = tuple.getPartitions();
    Assert.assertNotNull(parts);
    Assert.assertEquals(parts.length, 1);
    Assert.assertEquals(parts[0], 5);
    Assert.assertEquals("Windows", (long)tuple.getBaseSeconds() << 32 | tuple.getWindowId(), startingWindowId);

    serial = getSerializedRequest(null, id, down_type, upstream_id, 0, null, startingWindowId, 32 * 1024);
    tuple = (SubscribeRequestTuple)Tuple.getTuple(serial, 0, serial.length);

    Assert.assertEquals( "SubscriberId", tuple.getIdentifier(), id);
    Assert.assertEquals("UpstreamType", tuple.getStreamType(), down_type);
    Assert.assertEquals("UpstreamId", tuple.getUpstreamIdentifier(), upstream_id);
    Assert.assertEquals("Mask", tuple.getMask(), 0);
    Assert.assertEquals("BufferSize", tuple.getBufferSize(), 32 * 1024);
    Assert.assertNull(tuple.getPartitions());
    Assert.assertEquals("Window", (long)tuple.getBaseSeconds() << 32 | tuple.getWindowId(), startingWindowId);
  }
}
