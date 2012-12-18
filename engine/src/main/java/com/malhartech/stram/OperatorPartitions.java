package com.malhartech.stram;

import java.util.HashMap;
import java.util.Map;

import com.malhartech.api.DAG;
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


  static class PartitionImpl implements PartitionableOperator.Partition {
    private final PartitionPortMap partitionKeys;
    private final PartitionableOperator operator;

    PartitionImpl(PartitionableOperator operator, Map<InputPort<?>, PartitionKeys> partitionKeys) {
      this.operator = operator;
      this.partitionKeys = new PartitionPortMap();
      this.partitionKeys.putAll(partitionKeys);
      this.partitionKeys.modified = false;
    }

    PartitionImpl(PartitionableOperator operator) {
      this(operator, new PartitionPortMap());
    }

    @Override
    public Map<InputPort<?>, PartitionKeys> getPartitionKeys() {
      return partitionKeys;
    }

    @Override
    public int getLoad() {
      return 0;
    }

    @Override
    public PartitionableOperator getOperator() {
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


}
