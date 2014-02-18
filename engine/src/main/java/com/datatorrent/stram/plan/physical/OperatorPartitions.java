package com.datatorrent.stram.plan.physical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Partitionable;
import com.datatorrent.api.Partitionable.Partition;
import com.datatorrent.api.Partitionable.PartitionKeys;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * <p>OperatorPartitions class.</p>
 *
 * @since 0.3.2
 */
public class OperatorPartitions {

  /**
   * The default partitioning applied to operators that do not implement
   * {@link Partitionable} but are configured for partitioning in the
   * DAG.
   */
  public static class DefaultPartitioner
  {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitioner.class);

    public List<Partition<Operator>> defineInitialPartitions(LogicalPlan.OperatorMeta logicalOperator, int initialPartitionCnt)
    {
      List<Partition<Operator>> partitions = new ArrayList<Partition<Operator>>(initialPartitionCnt);
      for (int i=0; i<initialPartitionCnt; i++) {
        Partition<Operator> p = new DefaultPartition<Operator>(logicalOperator.getOperator());
        partitions.add(p);
      }

      Map<InputPortMeta, StreamMeta> inputs = logicalOperator.getInputStreams();
      if (!inputs.isEmpty()) {
        // partition the stream that was first connected in the DAG and send full data to remaining input ports
        // this gives control over which stream to partition under default partitioning to the DAG writer
        InputPortMeta portMeta = inputs.keySet().iterator().next();
        DefaultPartition.assignPartitionKeys(partitions, portMeta.getPortObject());
      }

      return partitions;
    }

    /**
     * Change existing partitioning based on runtime state (load). Unlike
     * implementations of {@link Partitionable}), decisions are made
     * solely based on load indicator and operator state is not
     * considered in the event of partition split or merge.
     *
     * @param partitions
     *          List of new partitions
     * @return
     */
    public List<Partition<Operator>> repartition(Collection<? extends Partition<Operator>> partitions) {
      List<Partition<Operator>> newPartitions = new ArrayList<Partition<Operator>>();
      HashMap<Integer, Partition<Operator>> lowLoadPartitions = new HashMap<Integer, Partition<Operator>>();
      for (Partition<Operator> p : partitions) {
        int load = p.getLoad();
        if (load < 0) {
          // combine neighboring underutilized partitions
          PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
          for (int partitionKey : pks.partitions) {
            // look for the sibling partition by excluding leading bit
            int reducedMask = pks.mask >>> 1;
            String lookupKey = Integer.valueOf(reducedMask) + "-" + Integer.valueOf(partitionKey & reducedMask);
            LOG.debug("pks {} lookupKey {}", pks, lookupKey);
            Partition<Operator> siblingPartition = lowLoadPartitions.remove(partitionKey & reducedMask);
            if (siblingPartition == null) {
              lowLoadPartitions.put(partitionKey & reducedMask, p);
            } else {
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
        } else if (load > 0) {
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
          } else {
            // assign keys to separate partitions
            newMask = e.getValue().mask;
            newKeys = e.getValue().partitions;
          }

          for (int key : newKeys) {
            Partition<Operator> newPartition = new DefaultPartition<Operator>(p.getPartitionedInstance());
            newPartition.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key)));
            newPartitions.add(newPartition);
          }
        } else {
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
     * @param partitions
     * @return
     */
    public static List<Partition<Operator>> repartitionInputOperator(Collection<? extends Partition<Operator>> partitions) {
      List<Partition<Operator>> newPartitions = new ArrayList<Partition<Operator>>();
      List<Partition<Operator>> lowLoadPartitions = new ArrayList<Partition<Operator>>();
      for (Partition<Operator> p : partitions) {
        int load = p.getLoad();
        if (load < 0) {
          if (!lowLoadPartitions.isEmpty()) {
            newPartitions.add(lowLoadPartitions.remove(0));
          } else {
            lowLoadPartitions.add(p);
          }
        } else if (load > 0) {
          newPartitions.add(new DefaultPartition<Operator>(p.getPartitionedInstance()));
          newPartitions.add(new DefaultPartition<Operator>(p.getPartitionedInstance()));
        } else {
          newPartitions.add(p);
        }
      }
      newPartitions.addAll(lowLoadPartitions);
      return newPartitions;
    }

  }

  public static Map<LogicalPlan.InputPortMeta, PartitionKeys> convertPartitionKeys(PTOperator oper, Map<InputPort<?>, PartitionKeys> portKeys)
  {
    if (portKeys == null) {
      return Collections.emptyMap();
    }
    HashMap<LogicalPlan.InputPortMeta, PartitionKeys> partitionKeys = Maps.newHashMapWithExpectedSize(portKeys.size());
    for (Map.Entry<InputPort<?>, PartitionKeys> portEntry : portKeys.entrySet()) {
      LogicalPlan.InputPortMeta pportMeta = oper.operatorMeta.getMeta(portEntry.getKey());
      if (pportMeta == null) {
        throw new AssertionError("Invalid port reference " + portEntry);
      }
      partitionKeys.put(pportMeta, portEntry.getValue());
    }
    return partitionKeys;
  }

}
