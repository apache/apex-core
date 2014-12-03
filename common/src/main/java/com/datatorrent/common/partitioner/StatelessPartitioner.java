/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.partitioner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Min;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Partitioner.Partition;

/**
 * This is a simple partitioner which creates partitionCount number of clones of an operator.
 * @param <T> The type of the operator
 */
public class StatelessPartitioner<T extends Operator> implements Partitioner<T>, Serializable
{
  private static final Logger logger = LoggerFactory.getLogger(StatelessPartitioner.class);
  private static final long serialVersionUID = 201411071710L;
  /**
   * The number of partitions for the default partitioner to create.
   */
  @Min(1)
  private int partitionCount = 1;

  /**
   * This creates a partitioner which creates only one partition.
   */
  public StatelessPartitioner()
  {
  }

  /**
   * This constructor is used to create the partitioner from a property.
   * @param value A string which is an integer of the number of partitions to create
   */
  public StatelessPartitioner(String value)
  {
    this(Integer.parseInt(value));
  }

  /**
   * This creates a partitioner which creates partitonCount partitions.
   * @param partitionCount The number of partitions to create.
   */
  public StatelessPartitioner(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method sets the number of partitions for the StatelessPartitioner to create.
   * @param partitionCount The number of partitions to create.
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * This method gets the number of partitions for the StatelessPartitioner to create.
   * @return The number of partitions to create.
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition<T>> definePartitions(Collection<Partition<T>> partitions, int incrementalCapacity)
  {
    int tempPartitionCount;

    //Do parallel partitioning
    if(incrementalCapacity != 0) {
      tempPartitionCount = incrementalCapacity;
    }
    //Do normal partitioning
    else {
      tempPartitionCount = partitionCount;
    }

    //Get a partition
    DefaultPartition<T> partition = (DefaultPartition<T>) partitions.iterator().next();

    logger.debug("entering define partitions, partitionCount {} incrementalCapacity {}",
                 tempPartitionCount,
                 incrementalCapacity);
    T operator = partitions.iterator().next().getPartitionedInstance();
    Collection<Partition<T>> newPartitions = null;

    //first call to define partitions
    if(partitions.iterator().next().getStats() == null) {
      logger.debug("first call to define partitions");
      newPartitions = Lists.newArrayList();

      for (int partitionCounter = 0;
              partitionCounter < tempPartitionCount;
              partitionCounter++) {
        newPartitions.add(new DefaultPartition<T>(operator));
      }

      // partition the stream that was first connected in the DAG and send full data to remaining input ports
      // this gives control over which stream to partition under default partitioning to the DAG writer
      List<InputPort<?>> inputPortList = partition.getInputPortList();
      //assign partition keys
      if (inputPortList != null && !inputPortList.isEmpty()) {
        DefaultPartition.assignPartitionKeys(newPartitions, inputPortList.iterator().next());
      }
    }
    else {
      logger.debug("subsequent call to define partitions");
      //define partitions is being called again

      //Repartitioning input operator
      if(partition.getPartitionKeys().isEmpty()) {
        newPartitions = repartitionInputOperator(partitions);
      }
      else {
        newPartitions = repartition(partitions);
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

  /**
   * Change existing partitioning based on runtime state (load). Unlike
   * implementations of {@link Partitioner}), decisions are made
   * solely based on load indicator and operator state is not
   * considered in the event of partition split or merge.
   *
   * @param partitions
   * List of new partitions
   * @return The new operators.
   */
  public static <T extends Operator> Collection<Partition<T>> repartition(Collection<Partition<T>> partitions)
  {
    List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
    HashMap<Integer, Partition<T>> lowLoadPartitions = new HashMap<Integer, Partition<T>>();
    for (Partition<T> p: partitions) {
      int load = p.getLoad();
      if (load < 0) {
        // combine neighboring underutilized partitions
        PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
        for (int partitionKey: pks.partitions) {
          // look for the sibling partition by excluding leading bit
          int reducedMask = pks.mask >>> 1;
          String lookupKey = Integer.valueOf(reducedMask) + "-" + Integer.valueOf(partitionKey & reducedMask);
          Partition<T> siblingPartition = lowLoadPartitions.remove(partitionKey & reducedMask);
          if (siblingPartition == null) {
            lowLoadPartitions.put(partitionKey & reducedMask, p);
          }
          else {
            // both of the partitions are low load, combine
            PartitionKeys newPks = new PartitionKeys(reducedMask, Sets.newHashSet(partitionKey & reducedMask));
            // put new value so the map gets marked as modified
            InputPort<?> port = siblingPartition.getPartitionKeys().keySet().iterator().next();
            siblingPartition.getPartitionKeys().put(port, newPks);
            // add as new partition
            newPartitions.add(siblingPartition);
            //LOG.debug("partition keys after merge {}", siblingPartition.getPartitionKeys());
          }
        }
      }
      else if (load > 0) {
        // split bottlenecks
        Map<InputPort<?>, PartitionKeys> keys = p.getPartitionKeys();
        Map.Entry<InputPort<?>, PartitionKeys> e = keys.entrySet().iterator().next();

        final int newMask;
        final Set<Integer> newKeys;

        if (e.getValue().partitions.size() == 1) {
          // split single key
          newMask = (e.getValue().mask << 1) | 1;
          int key = e.getValue().partitions.iterator().next();
          int key2 = (newMask ^ e.getValue().mask) | key;
          newKeys = Sets.newHashSet(key, key2);
        }
        else {
          // assign keys to separate partitions
          newMask = e.getValue().mask;
          newKeys = e.getValue().partitions;
        }

        for (int key: newKeys) {
          Partition<T> newPartition = new DefaultPartition<T>(p.getPartitionedInstance());
          newPartition.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key)));
          newPartitions.add(newPartition);
        }
      }
      else {
        // leave unchanged
        newPartitions.add(p);
      }
    }
    // put back low load partitions that could not be combined
    newPartitions.addAll(lowLoadPartitions.values());
    return newPartitions;
  }

  /**
   * Adjust the partitions of an input operator (operator with no connected input stream).
   *
   * @param <T> The operator type
   * @param partitions
   * @return The new operators.
   */
  public static <T extends Operator> Collection<Partition<T>> repartitionInputOperator(Collection<Partition<T>> partitions)
  {
    List<Partition<T>> newPartitions = new ArrayList<Partition<T>>();
    List<Partition<T>> lowLoadPartitions = new ArrayList<Partition<T>>();
    for (Partition<T> p: partitions) {
      int load = p.getLoad();
      if (load < 0) {
        if (!lowLoadPartitions.isEmpty()) {
          newPartitions.add(lowLoadPartitions.remove(0));
        }
        else {
          lowLoadPartitions.add(p);
        }
      }
      else if (load > 0) {
        newPartitions.add(new DefaultPartition<T>(p.getPartitionedInstance()));
        newPartitions.add(new DefaultPartition<T>(p.getPartitionedInstance()));
      }
      else {
        newPartitions.add(p);
      }
    }
    newPartitions.addAll(lowLoadPartitions);
    return newPartitions;
  }
}
