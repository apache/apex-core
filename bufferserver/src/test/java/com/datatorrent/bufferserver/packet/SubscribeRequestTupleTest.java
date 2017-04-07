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
import org.testng.annotations.Test;

import static com.datatorrent.bufferserver.packet.SubscribeRequestTuple.getSerializedRequest;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

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
    ArrayList<Integer> partitions = new ArrayList<>();
    partitions.add(5);
    long startingWindowId = 0xcafebabe00000078L;

    byte[] serial = getSerializedRequest(null, id, down_type, upstream_id, mask, partitions, startingWindowId, 32 * 1024);
    SubscribeRequestTuple tuple = (SubscribeRequestTuple)Tuple.getTuple(serial, 0, serial.length);

    assertEquals(tuple.getIdentifier(), id, "Identifier");
    assertEquals(tuple.getStreamType(), down_type, "UpstreamType");
    assertEquals(tuple.getUpstreamIdentifier(), upstream_id, "UpstreamId");
    assertEquals(tuple.getMask(), mask, "Mask");
    assertEquals(tuple.getBufferSize(), 32 * 1024, "BufferSize");
    int[] parts = tuple.getPartitions();
    assertNotNull(parts);
    assertEquals(parts.length, 1);
    assertEquals(parts[0], 5);
    assertEquals((long)tuple.getBaseSeconds() << 32 | tuple.getWindowId(), startingWindowId, "Window");

    serial = getSerializedRequest(null, id, down_type, upstream_id, 0, null, startingWindowId, 32 * 1024);
    tuple = (SubscribeRequestTuple)Tuple.getTuple(serial, 0, serial.length);

    assertEquals(tuple.getIdentifier(), id, "Identifier");
    assertEquals(tuple.getStreamType(), down_type, "UpstreamType");
    assertEquals(tuple.getUpstreamIdentifier(), upstream_id, "UpstreamId");
    assertEquals(tuple.getMask(), 0, "Mask");
    assertEquals(tuple.getBufferSize(), 32 * 1024, "BufferSize");
    assertNull(tuple.getPartitions());
    assertEquals((long)tuple.getBaseSeconds() << 32 | tuple.getWindowId(), startingWindowId, "Window");
  }
}
