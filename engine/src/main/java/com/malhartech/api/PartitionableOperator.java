/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import com.malhartech.api.Operator.InputPort;
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
   * @param partitions - Partitions associated with available containers.
   *
   * @return new partitions. The partitions which should not be changed can be returned
   * as they are.
   */
  public List<Partition> redoPartitions(List<Partition> partitions);

  public interface Partition extends Map<InputPort, Set<byte[]>>
  {
    /**
     * Get an indication of the load handled by this particular partition.
     *
     * @return An positive integer is an indicative of the load handled by the partition.
     */
    public int getLoad();

    /**
     * Get the frozen state of the partition which is currently handling the partition.
     *
     * @return frozen operator instance
     */
    public PartitionableOperator getOperator();

    /**
     * Set the new state of the operator which will handle the partitions.
     *
     * @param new state of the operator
     */
    public void setOperator(PartitionableOperator operator);
  }
}
