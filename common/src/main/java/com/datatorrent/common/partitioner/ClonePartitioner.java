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
package com.datatorrent.common.partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;

/**
 * This is a simple partitioner, which replicates data across all partitions of an operator
 *
 * @param <T> The type of the operator
 * @since 2.0.0
 */
public class ClonePartitioner<T extends Operator> implements Partitioner<T>, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(ClonePartitioner.class);
  
  //TODO How is this generated?
  private static final long serialVersionUID = 201462371710L;
  /**
   * The number of partitions for the default partitioner to create.
   */
  @Min(1)
  private int partitionCount = 1;

  /**
   * This creates a partitioner which creates only one partition.
   */
  public ClonePartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   * @param value A string which is an integer of the number of partitions to create
   */
  public ClonePartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   * @param partitionCount The number of partitions to create.
   */
  public ClonePartitioner(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method sets the number of partitions for the ClonePartitioner to create.
   * @param partitionCount The number of partitions to create.
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method gets the number of partitions for the ClonePartitioner to create.
   * @return The number of partitions to create.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  @Override
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, PartitioningContext context)
  {
    final int newPartitionCount = DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);
    logger.debug("define partitions, partitionCount current {} requested {}", partitions.size(), newPartitionCount);

    //Get a partition
    DefaultPartition<T> partition = (DefaultPartition<T>)partitions.iterator().next();
    Collection<Partition<T>> newPartitions;

    // first call to define partitions
    newPartitions = Lists.newArrayList();

    // Add as many partitions as we wish to have
    for (int partitionCounter = 0; partitionCounter < newPartitionCount; partitionCounter++) {
      DefaultPartition<T> partitionToAdd = new DefaultPartition<T>(partition.getPartitionedInstance());
      newPartitions.add(partitionToAdd);
    }

    // For every partition, accept all data from all the input ports by defining a mask that accepts all data
    List<InputPort<?>> inputPortList = context.getInputPorts();
    if (inputPortList != null) {
      for (InputPort<?> port : inputPortList) {
        int partitionMask = 0xffffffff; // We don't want to filter data, so use a mask that allows all values

        for (Partition<T> p : newPartitions) {
          p.getPartitionKeys().put(port, new PartitionKeys(partitionMask, Sets.newHashSet(0)));
        }
      }
    }

    logger.debug("new partition size {}", newPartitions.size());
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<T>> partitions)
  {
    //Do nothing
  }
}
