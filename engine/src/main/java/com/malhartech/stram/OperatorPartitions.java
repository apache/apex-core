package com.malhartech.stram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.InputPortMeta;
import com.malhartech.api.DAG.StreamDecl;
import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.PartitionableOperator.Partition;
import com.malhartech.api.PartitionableOperator.PartitionKeys;

public class OperatorPartitions {

  final DAG.OperatorWrapper operatorWrapper;

  public OperatorPartitions(DAG.OperatorWrapper operator) {
    this.operatorWrapper = operator;
  }

  private Map<DAG.InputPortMeta, PartitionKeys> convertMapping(Map<InputPort<?>, PartitionKeys> keys) {
    Map<DAG.InputPortMeta, PartitionKeys> partitionKeys;
    partitionKeys = new HashMap<DAG.InputPortMeta, PartitionKeys>(keys.size());
    Map<InputPort<?>, PartitionKeys> partKeys = keys;
    for (Map.Entry<InputPort<?>, PartitionKeys> portEntry : partKeys.entrySet()) {
      DAG.InputPortMeta pportMeta = operatorWrapper.getInputPortMeta(portEntry.getKey());
      if (pportMeta == null) {
        throw new IllegalArgumentException("Invalid port reference " + portEntry);
      }
      partitionKeys.put(pportMeta, portEntry.getValue());
    }
    return partitionKeys;
  }


  public static class PartitionImpl implements PartitionableOperator.Partition {
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
    public Partition getInstance(PartitionableOperator operator) {
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

    public List<Partition> defineInitialPartitions(DAG.OperatorWrapper logicalOperator, int initialPartitionCnt) {

      //int partitionBits = 0;
      //if (initialPartitionCnt > 0) {
      //  partitionBits = 1 + (int) (Math.log(initialPartitionCnt) / Math.log(2)) ;
      //}
      int partitionBits = (Integer.numberOfLeadingZeros(0)-Integer.numberOfLeadingZeros(initialPartitionCnt-1));
      int partitionMask = 0;
      if (partitionBits > 0) {
        partitionMask = -1 >>> (Integer.numberOfLeadingZeros(-1)) - partitionBits;
      }

      List<Partition> partitions = new ArrayList<Partition>(initialPartitionCnt);
      for (int i=0; i<initialPartitionCnt; i++) {
        Partition p = new PartitionImpl(logicalOperator.getOperator());
        // default mapping partitions the stream that was first connected in the DAG and send full data to remaining input ports
        // this gives control over which stream to partition with the default partitioning to the DAG writer
        Map<InputPortMeta, StreamDecl> inputs = logicalOperator.getInputStreams();
        if (inputs.size() == 0) {
          // TODO - allow input operator partitioning?
          throw new AssertionError("Partitioning configured for operator but no input ports found: " + logicalOperator);
        }
        // TODO: eliminate this and work with the port meta object instead as this is what we will be using during plan processing anyways
        InputPortMeta portMeta = inputs.keySet().iterator().next();
        p.getPartitionKeys().put(portMeta.getPortObject(), new PartitionKeys(partitionMask, Sets.newHashSet(i)));
        partitions.add(p);
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
    public List<Partition> repartition(List<? extends Partition> partitions) {
      List<Partition> newPartitions = new ArrayList<Partition>();
      HashMap<Integer, Partition> lowLoadPartitions = new HashMap<Integer, Partition>();
      for (Partition p : partitions) {
        int load = p.getLoad();
        if (load < 0) {
          // combine neighboring underutilized partitions
          PartitionKeys pks = p.getPartitionKeys().values().iterator().next(); // one port partitioned
          int partitionKey = pks.partitions.iterator().next();

          // look for the sibling partition by flipping leading bit
          int lookupKey = ( ( pks.mask >>> 1 ) & pks.mask ) & partitionKey;
          Partition siblingPartition = lowLoadPartitions.get(lookupKey);
          if (siblingPartition == null) {
            lowLoadPartitions.put(partitionKey, p);
          } else {
            // both of the partitions are low load, combine
            lowLoadPartitions.remove(lookupKey);
            PartitionKeys newPks = new PartitionKeys(pks.mask >>> 1, Sets.newHashSet(lookupKey & (pks.mask >>> 1)));
            siblingPartition.getPartitionKeys().entrySet().iterator().next().setValue(newPks);
            // add as new partition
            // TODO: we should repeat the same check with for the combined partition,
            // which would catch the case where other branch was combined, too
            newPartitions.add(siblingPartition);
          }
        } else if (load > 0) {
          // split bottlenecks
          Map<InputPort<?>, PartitionKeys> keys = p.getPartitionKeys();
          Map.Entry<InputPort<?>, PartitionKeys> e = keys.entrySet().iterator().next();
          // default partitions always have a single key
          int key = e.getValue().partitions.iterator().next();
          int newMask = (e.getValue().mask << 1) | 1;
          int key2 = (newMask ^ e.getValue().mask) | key;

          Partition p1 = new PartitionImpl(p.getOperator());
          p1.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key)));

          Partition p2 = new PartitionImpl(p.getOperator());
          p2.getPartitionKeys().put(e.getKey(), new PartitionKeys(newMask, Sets.newHashSet(key2)));

          newPartitions.add(p1);
          newPartitions.add(p2);
        } else {
          // leave unchanged
          newPartitions.add(p);
        }
      }
      return newPartitions;
    }

  }

}
