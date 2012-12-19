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
    public String toString() {
      return "[" + mask + "," + partitions + "]";
    }

  }

  public interface Partition
  {
    /**
     * Return the partition keys for this partition.
     * Input ports that are not mapped will receive all data.
     * @return
     */
    public Map<InputPort<?>, PartitionKeys> getPartitionKeys();

    /**
     * Get an indication of the load handled by this particular partition.
     *
     * @return An positive integer is an indicative of the load handled by the partition.
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
     * @return
     */
    public Partition getInstance(PartitionableOperator operator);
  }
}
