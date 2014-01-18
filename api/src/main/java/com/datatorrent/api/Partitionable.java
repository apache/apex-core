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
import java.util.Map;
import java.util.Set;

import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.api.Operator.InputPort;

/**
 * Components which want to have control over how they are partitioned may implement
 * Partitionable interface and direct the engine to partition them in a certain way.
 *
 * @param <T> type for which custom partitions need to be defined.
 * @since 0.3.2
 */
public interface Partitionable<T extends Operator>
{
  /**
   * Give an opportunity to the operator to decide how it would like to clone
   * itself into multiple copies so that they all collectively can do the work
   * by working on only a partition of the data.
   * <p>
   * Through definePartitions the operator is also notified of presence of or
   * lack of the addtionalCapacity the cluster has for more instances of the
   * operator. If this capacity is positive then the operator can redistribute
   * its load among the current instances and the newly defined instances. If
   * this capacity is negative, then the operator should consider scaling down
   * by releasing a few instances. The number of instances released are ideally
   * at least equal to absolute value of incrementalCapacity. If this capacity
   * is zero, then the operator should look at repartitioning among the existing
   * instances as the current distribution of load is unfair.
   * <p>
   * The list of existing partitions reflects the last checkpoint state for each
   * of the operator instances. When creating new partitions, the implementation
   * has the opportunity to transfer state from these existing operator
   * instances to new instances. At minimum, properties set at initialization
   * time on the original operator need to be set on new instances.
   *
   * @param partitions
   *                            - Current set of partitions
   * @param incrementalCapacity -
   *                            The count of more instances of this operator can the
   *                            infrastructure support. If this number is positive,
   * @return New partitioning. Partitions from input list which should not be
   *         changed can be returned as they are.
   */
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, int incrementalCapacity);

  /**
   * Interface to be implemented by operator that wants to be notified about its effective partitioning.
   * Called by the engine after requested partitioning is applied to the physical plan.
   * Allows the operator to track stats of individual partitions by id.
   *
   * @param <T> - Type of the operator
   * @see {@link StatsListener}
   * @see {@link Partitionable#definePartitions}
   */
  interface PartitionAware<T extends Operator>
  {
    void partitioned(Map<Integer, Partition<T>> partitions);

  }

  public class PartitionKeys implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312271835L;
    final public int mask;
    final public Set<Integer> partitions;

    public PartitionKeys(int mask, Set<Integer> partitions)
    {
      this.mask = mask;
      this.partitions = partitions;
    }

    @Override
    public int hashCode()
    {
      int hash = 7;
      hash = 79 * hash + this.mask;
      hash = 79 * hash + (this.partitions != null ? this.partitions.hashCode() : 0);
      return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final PartitionKeys other = (PartitionKeys)obj;
      if (this.mask != other.mask) {
        return false;
      }
      return this.partitions == other.partitions || (this.partitions != null && this.partitions.equals(other.partitions));
    }

    @Override
    public String toString()
    {
      return "[" + mask + "," + partitions + "]";
    }

  }

  public interface Partition<PARTITIONABLE extends Operator>
  {
    /**
     * Return the partition keys for this partition.
     * Input ports that are not mapped will receive all data.
     *
     * @return Map<InputPort<?>, PartitionKeys>
     */
    public Map<InputPort<?>, PartitionKeys> getPartitionKeys();

    /**
     * Get an indication of the load handled by this partition. The indicator
     * is calculated by the platform based on throughput etc. Thresholds are
     * configured in the DAG as attributes.<br>
     * Negative number: partition is under utilized and could me merged.<br>
     * Zero: the partition is operating within the load thresholds.<br>
     * Positive: partition is handling more load (and is a candidate for partition split).<br>
     *
     * @return Integer indicative of the load handled by the partition.
     */
    public int getLoad();

    /**
     * Get the latest statistics for this partition. Null when no stats have been collected yet.
     * <p>
     * Stats would typically be used to determine utilization and decide whether partitions should be merged or split.
     * Through {@link Stats.OperatorStats.CustomStats} operator implementation specific stats can be collected and used
     * to derive optimal partitioning.
     *
     * @return
     */
    public BatchedOperatorStats getStats();

    /**
     * Get the frozen state of the operator which is currently handling the partition.
     *
     * @return frozen operator instance
     */
    public PARTITIONABLE getPartitionedInstance();

    /**
     * Get the attributes associated with this partition.
     * The returned map does not contain any attributes that may have been defined in the parent context of this context.
     *
     * @return attributes defined for the current context.
     */
    public AttributeMap getAttributes();

  }

}
