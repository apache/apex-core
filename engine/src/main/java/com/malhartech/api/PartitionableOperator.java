/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import java.util.List;
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
   * @param partitions - Current partitions, containing at least one entry.
   *
   * @return New partitioning. Partitions from input list which should not be changed can be returned
   * as they are.
   */
  public List<Partition> definePartitions(List<? extends Partition> partitions);

  public class PartitionKeys
  {
    final public int mask;
    final public Set<Integer> partitions;

    public PartitionKeys(int mask, Set<Integer> partitions) {
      this.mask = mask;
      this.partitions = partitions;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof PartitionKeys)) {
        return false;
      }
      PartitionKeys pks = (PartitionKeys)obj;
      return (this.mask == pks.mask && this.partitions.equals(pks.partitions));
    }

    @Override
    public int hashCode() {
      return mask + partitions.size();
    }

    @Override
    public String toString() {
      return "[" + mask + "," + partitions + "]";
    }
  }

  public interface Partition
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
    public Operator getOperator();

    /**
     * Create a new partition for the given operator. The returned partition
     * needs to be further configured with the port to partition key mapping.
     *
     * @param operator
     * @return Partition
     */
    public Partition getInstance(PartitionableOperator operator);
  }
}
