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

import java.util.HashSet;
import java.util.Set;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;

/**
 * <p>PartitionAwareSinkForPersistence class.</p>
 *
 * @since 3.2.0
 */
public class PartitionAwareSinkForPersistence extends PartitionAwareSink<Object>
{
  StreamCodecWrapperForPersistance<Object> serdeForPersistence;

  public PartitionAwareSinkForPersistence(StreamCodecWrapperForPersistance<Object> serde, Set<Integer> partitions, int mask, Sink<Object> output)
  {
    super(serde, partitions, mask, output);
    serdeForPersistence = serde;
  }

  public PartitionAwareSinkForPersistence(StreamCodecWrapperForPersistance<Object> serde, int mask, Sink<Object> output)
  {
    // If partition keys is null, everything should be passed to sink
    super(serde, createPartitionKeys(mask), mask, output);
    serdeForPersistence = serde;
  }

  private static Set<Integer> createPartitionKeys(int mask)
  {
    Set<Integer> partitions = new HashSet<>();
    // Add all entries in mask to partitions keys
    for (int i = 0; i <= mask; i++) {
      partitions.add(i);
    }
    return partitions;
  }

  @Override
  protected boolean canSendToOutput(Object payload)
  {
    if (!serdeForPersistence.shouldCaptureEvent(payload)) {
      return false;
    }

    return super.canSendToOutput(payload);
  }
}
