package com.datatorrent.stram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.InputPortMeta;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.google.common.collect.Sets;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.PartitionableOperator;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.PartitionableOperator.PartitionKeys;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>OperatorPartitions class.</p>
 *
 * @since 0.3.2
 */
public class OperatorPartitions {

  final LogicalPlan.OperatorMeta operatorWrapper;

  public OperatorPartitions(LogicalPlan.OperatorMeta operator) {
    this.operatorWrapper = operator;
  }

  public static class PartitionImpl implements PartitionableOperator.Partition<Operator> {
    private final PartitionPortMap partitionKeys;
    private final Operator operator;
    private final int loadIndicator;

    PartitionImpl(Operator operator, Map<InputPort<?>, PartitionKeys> partitionKeys, int loadIndicator) {
      this.operator = operator;
      this.partitionKeys = new PartitionPortMap();
      this.partitionKeys.putAll(partitionKeys);
      this.partitionKeys.modified = false;
      this.loadIndicator = loadIndicator;
    }

    PartitionImpl(Operator operator) {
      this(operator, new PartitionPortMap(), 0);
    }

    @Override
    public Map<InputPort<?>, PartitionKeys> getPartitionKeys() {
      return partitionKeys;
    }

    @Override
    public int getLoad() {
      return this.loadIndicator;
    }

    @Override
    public Operator getOperator() {
      return operator;
    }

    @Override
    public Partition<Operator> getInstance(Operator operator) {
      return new PartitionImpl(operator);
    }

    boolean isModified() {
      return partitionKeys.modified;
    }

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

      for (Integer bb: collection1.partitions) {
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
      for (Map.Entry<? extends InputPort<?>, ? extends PartitionKeys> entry: m.entrySet()) {
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
   * The default partitioning applied to operators that do not implement
   * {@link PartitionableOperator} but are configured for partitioning in the
   * DAG.
   */
  public static class DefaultPartitioner {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPartitioner.class);

    public List<Partition<?>> defineInitialPartitions(LogicalPlan.OperatorMeta logicalOperator, int initialPartitionCnt) {

      //int partitionBits = 0;
      //if (initialPartitionCnt > 0) {
      //  partitionBits = 1 + (int) (Math.log(initialPartitionCnt) / Math.log(2)) ;
      //}
      int partitionBits = (Integer.numberOfLeadingZeros(0)-Integer.numberOfLeadingZeros(initialPartitionCnt-1));
      int partitionMask = 0;
      if (partitionBits > 0) {
        partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
      }

      List<Partition<?>> partitions = new ArrayList<Partition<?>>(initialPartitionCnt);
      for (int i=0; i<initialPartitionCnt; i++) {
        Partition<?> p = new PartitionImpl(logicalOperator.getOperator());
        partitions.add(p);
      }

      if (!(logicalOperator.getOperator() instanceof InputOperator)) {
        Map<InputPortMeta, StreamMeta> inputs = logicalOperator.getInputStreams();
        if (inputs.isEmpty()) {
          throw new AssertionError("Partitioning configured for operator but no input ports connected: " + logicalOperator);
        }
        InputPortMeta portMeta = inputs.keySet().iterator().next();

        for (int i=0; i<=partitionMask; i++) {
          // partition the stream that was first connected in the DAG and send full data to remaining input ports
          // this gives control over which stream to partition under default partitioning to the DAG writer
          Partition<?> p = partitions.get(i % partitions.size());
          PartitionKeys pks = p.getPartitionKeys().get(portMeta.getPortObject());
          if (pks == null) {
            // TODO: work with the port meta object instead as this is what we will be using during plan processing anyways
            p.getPartitionKeys().put(portMeta.getPortObject(), new PartitionKeys(partitionMask, Sets.newHashSet(i)));
          } else {
            pks.partitions.add(i);
          }
        }
      }

      return partitions;
    }

    /**
     * Change existing partitioning based on runtime state (load). Unlike
     * implementations of {@link PartitionableOperator}), decisions are made
     * solely based on load indicator and state of operator instances is not
     * considered in the event of partition split or merge.
     *
     * @param partitions
     *          List of new partitions
     * @return
     */
    public List<Partition<?>> repartition(Collection<? extends Partition<?>> partitions) {
      List<Partition<?>> newPartitions = new ArrayList<Partition<?>>();
      HashMap<Integer, Partition<?>> lowLoadPartitions = new HashMap<Integer, Partition<?>>();
      for (Partition<?> p : partitions) {
        int load = p.getLoad();
        if (load < 0) {
          // combine neighboring underutilized partitions
          PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
          for (int partitionKey : pks.partitions) {
            // look for the sibling partition by excluding leading bit
            int reducedMask = pks.mask >>> 1;
            String lookupKey = Integer.valueOf(reducedMask) + "-" + Integer.valueOf(partitionKey & reducedMask);
            LOG.debug("pks {} lookupKey {}", pks, lookupKey);
            Partition<?> siblingPartition = lowLoadPartitions.remove(partitionKey & reducedMask);
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
            Partition<?> newPartition = new PartitionImpl(p.getOperator());
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

  }

}
