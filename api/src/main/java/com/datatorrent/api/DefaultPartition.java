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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.Sets;

import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;

/**
 * <p>
 * DefaultPartition class.</p>
 *
 * @param <T>
 * @since 0.9.1
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

  /**
   * Assign partitions keys for the given list of partitions and port of the logical operator.
   * <p>
   * The incoming stream will be partitioned by n keys, with n the nearest power of 2 greater or equal to the
   * number of partition instances provided. If the number of instances does not align with a power of 2, some of the
   * partitions will be assigned 2 keys. This logic is used for default partitioning and can be used to implement
   * {@link Partitionable}.
   *
   * @param partitions
   * @param inputPort
   */
  public static void assignPartitionKeys(Collection<Partition<?>> partitions, InputPort<?> inputPort)
  {
    if (partitions.isEmpty()) {
      throw new IllegalArgumentException("partitions collection cannot be empty");
    }

    int partitionBits = (Integer.numberOfLeadingZeros(0) - Integer.numberOfLeadingZeros(partitions.size() - 1));
    int partitionMask = 0;
    if (partitionBits > 0) {
      partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
    }
    
    Iterator<Partition<?>> iterator = partitions.iterator();
    for (int i = 0; i <= partitionMask; i++) {
      Partition<?> p;
      if (iterator.hasNext()) {
        p = iterator.next();
      }
      else {
        iterator = partitions.iterator();
        p = iterator.next();
      }

      PartitionKeys pks = p.getPartitionKeys().get(inputPort);
      if (pks == null) {
        p.getPartitionKeys().put(inputPort, new PartitionKeys(partitionMask, Sets.newHashSet(i)));
      }
      else {
        pks.partitions.add(i);
      }
    }
  }

}
