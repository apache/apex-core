/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface PartitionableOperator extends Operator
{
  /**
   * Give an opportunity to the operator to decide how it would like to clone itself
   * into multiple copies so that they all collectively can do the work by working on
   * only a partition of the data.
   *
   * Through definePartitions the operator is also notified of presence of or lack of
   * the addtionalCapacity the cluster has for more instances of the operator.
   * If this capacity is positive then the operator can redistribute its load among
   * the current instances and the newly defined instances.
   * If this capacity is negative, then the operator should consider scaling down by
   * releasing a few instances. The number of instances released are ideally at least
   * equal to absolute value of incrementalCapacity.
   * If this capacity is zero, then the operator should relook at repartitioning among
   * the existing instances as the current distribution of load is unfair.
   *
   * @param partitions - Current set of partitions
   * @param incrementalCapacity The count of more instances of this operator can the infrastructure support. If this number is positive,
   * @return New partitioning. Partitions from input list which should not be changed can be returned as they are.
   */
  public Collection<Partition<?>> definePartitions(Collection<? extends Partition<?>> partitions, int incrementalCapacity);

  public class PartitionKeys
  {
    final public int mask;
    final public Set<Integer> partitions;

    public PartitionKeys(int mask, Set<Integer> partitions) {
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
      if (this.partitions != other.partitions && (this.partitions == null || !this.partitions.equals(other.partitions))) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "[" + mask + "," + partitions + "]";
    }
  }

  public interface Partition<OPERATOR extends Operator>
  {
    /**
     * Return the partition keys for this partition.
     * Input ports that are not mapped will receive all data.
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
     * Get the frozen state of the operator which is currently handling the partition.
     *
     * @return frozen operator instance
     */
    public OPERATOR getOperator();

    /**
     * Create a new partition for the given operator. The returned partition
     * needs to be further configured with the port to partition key mapping.
     *
     * @param operator
     * @return Partition
     */
    public Partition<OPERATOR> getInstance(OPERATOR operator);
  }
}
