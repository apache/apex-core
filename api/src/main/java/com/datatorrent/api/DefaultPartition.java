  /*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitionable.PartitionKeys;

/**
 * <p>DefaultPartition class.</p>
 *
 * @since 0.9.0
 */
public class DefaultPartition<T extends Operator> implements Partitionable.Partition<T>
{
  private final PartitionPortMap partitionKeys;
  private final T operator;
  private final int loadIndicator;
  private final AttributeMap attributes = new DefaultAttributeMap();
  private final BatchedOperatorStats stats;

  public DefaultPartition(T operator, Map<InputPort<?>, PartitionKeys> partitionKeys, int loadIndicator, BatchedOperatorStats stats)
  {
    this.operator = operator;
    this.partitionKeys = new PartitionPortMap();
    this.partitionKeys.putAll(partitionKeys);
    this.partitionKeys.modified = false;
    this.loadIndicator = loadIndicator;
    this.stats = stats;
  }

  public DefaultPartition(T operator)
  {
    this(operator, new PartitionPortMap(), 0, null);
  }

  @Override
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public Map<InputPort<?>, PartitionKeys> getPartitionKeys()
  {
    return partitionKeys;
  }

  @Override
  public int getLoad()
  {
    return this.loadIndicator;
  }

  @Override
  public BatchedOperatorStats getStats()
  {
    return this.stats;
  }

  @Override
  public T getPartitionedInstance()
  {
    return operator;
  }

  public boolean isModified()
  {
    return partitionKeys.modified;
  }

  @Override
  public AttributeMap getAttributes()
  {
    return attributes;
  }

  public static class PartitionPortMap extends HashMap<InputPort<?>, PartitionKeys>
  {
    private static final long serialVersionUID = 201212131624L;
    private boolean modified;

    private boolean validateEqual(PartitionKeys collection1, PartitionKeys collection2)
    {
      if (collection1 == null && collection2 == null) {
        return true;
      }

      if (collection1 == null || collection2 == null) {
        return false;
      }

      if (collection1.mask != collection2.mask) {
        return false;
      }

      if (collection1.partitions.size() != collection2.partitions.size()) {
        return false;
      }

      for (Integer bb : collection1.partitions) {
        if (!collection2.partitions.contains(bb)) {
          return false;
        }
      }
      return true;
    }

    public boolean isModified()
    {
      return modified;
    }

    @Override
    public PartitionKeys put(InputPort<?> key, PartitionKeys value)
    {
      PartitionKeys prev = super.put(key, value);
      if (!modified) {
        modified = !validateEqual(prev, value);
      }

      return prev;
    }

    @Override
    public void putAll(Map<? extends InputPort<?>, ? extends PartitionKeys> m)
    {
      for (Map.Entry<? extends InputPort<?>, ? extends PartitionKeys> entry : m.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    @SuppressWarnings("element-type-mismatch")
    public PartitionKeys remove(Object key)
    {
      if (containsKey(key)) {
        modified = true;
        return super.remove(key);
      }

      return null;
    }

    @Override
    public void clear()
    {
      if (!isEmpty()) {
        modified = true;
        super.clear();
      }
    }

  }

}
