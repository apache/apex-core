/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.bufferserver.packet;

import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    byte[] serial = SubscribeRequestTuple.getSerializedRequest(null, id, down_type, upstream_id, mask, partitions, startingWindowId, 0);
    SubscribeRequestTuple tuple = (SubscribeRequestTuple)Tuple.getTuple(serial, 0, serial.length);
    Assert.assertEquals(tuple.getIdentifier(), id, "Identifier");
    Assert.assertEquals(tuple.getStreamType(), down_type, "UpstreamType");
    Assert.assertEquals(tuple.getUpstreamIdentifier(), upstream_id, "UpstreamId");
    Assert.assertEquals(tuple.getMask(), mask, "Mask");

    int[] parts = tuple.getPartitions();
    Assert.assertTrue(parts != null && parts.length == 1 && parts[0] == 5);

    Assert.assertEquals(Long.toHexString((long)tuple.getBaseSeconds() << 32 | tuple.getWindowId()), Long.toHexString(startingWindowId), "Window");
  }

}