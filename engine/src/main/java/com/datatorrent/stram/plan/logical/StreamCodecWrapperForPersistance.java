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
package com.datatorrent.stram.plan.logical;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datatorrent.api.Partitioner.PartitionKeys;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;

/**
 * <p>StreamCodecWrapperForPersistance class.</p>
 *
 * @since 3.2.0
 */
public class StreamCodecWrapperForPersistance<T> implements StreamCodec<T>, Serializable
{

  private StreamCodec<Object> specifiedStreamCodec;
  public Map<InputPortMeta, Collection<PartitionKeys>> inputPortToPartitionMap;
  public Map<InputPortMeta, StreamCodec<Object>> codecsToMerge;
  private boolean operatorPartitioned;

  public StreamCodecWrapperForPersistance(Map<InputPortMeta, StreamCodec<Object>> inputStreamCodecs, StreamCodec<Object> specifiedStreamCodec)
  {
    this.codecsToMerge = inputStreamCodecs;
    this.setSpecifiedStreamCodec(specifiedStreamCodec);
    inputPortToPartitionMap = new HashMap<>();
  }

  @Override
  public Object fromByteArray(Slice fragment)
  {
    return getSpecifiedStreamCodec().fromByteArray(fragment);
  }

  @Override
  public Slice toByteArray(T o)
  {
    return getSpecifiedStreamCodec().toByteArray(o);
  }

  @Override
  public int getPartition(T o)
  {
    return getSpecifiedStreamCodec().getPartition(o);
  }

  public boolean shouldCaptureEvent(T o)
  {
    for (Entry<InputPortMeta, Collection<PartitionKeys>> entry : inputPortToPartitionMap.entrySet()) {
      StreamCodec<Object> codec = codecsToMerge.get(entry.getKey());
      Collection<PartitionKeys> partitionKeysList = entry.getValue();

      for (PartitionKeys keys : partitionKeysList) {
        if ( keys.partitions != null && keys.partitions.contains(keys.mask & codec.getPartition(o))) {
          // Then at least one of the partitions is getting this event
          // So send the event to persist operator
          return true;
        }
      }
    }

    return false;
  }

  public StreamCodec<Object> getSpecifiedStreamCodec()
  {
    if (specifiedStreamCodec == null) {
      specifiedStreamCodec = new DefaultKryoStreamCodec();
    }
    return specifiedStreamCodec;
  }

  public void setSpecifiedStreamCodec(StreamCodec<Object> specifiedStreamCodec)
  {
    this.specifiedStreamCodec = specifiedStreamCodec;
  }

  public boolean isOperatorPartitioned()
  {
    return operatorPartitioned;
  }

  public void setOperatorPartitioned(boolean operatorPartitioned)
  {
    this.operatorPartitioned = operatorPartitioned;
  }

}
