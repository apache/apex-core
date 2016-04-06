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

import java.util.Set;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>PartitionAwareSink class.</p>
 *
 * @param <T>
 * @since 0.3.2
 */
public class PartitionAwareSink<T> implements Sink<T>
{
  private final StreamCodec<T> serde;
  private final Set<Integer> partitions;
  private final int mask;
  private volatile Sink<T> output;
  private int count;

  /**
   *
   * @param serde
   * @param partitions
   * @param mask
   * @param output
   */
  public PartitionAwareSink(StreamCodec<T> serde, Set<Integer> partitions, int mask, Sink<T> output)
  {
    this.serde = serde;
    this.partitions = partitions;
    this.output = output;
    this.mask = mask;
  }

  /**
   *
   * @param payload
   */
  @Override
  public void put(T payload)
  {
    if (payload instanceof Tuple) {
      count++;
      output.put(payload);
    } else if (canSendToOutput(payload)) {
      count++;
      output.put(payload);
    }
  }

  protected boolean canSendToOutput(T payload)
  {
    return partitions.contains(serde.getPartition(payload) & mask);
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    } finally {
      if (reset) {
        count = 0;
      }
    }
  }

}
