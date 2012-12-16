package com.malhartech.stram;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.malhartech.api.DAG;
import com.malhartech.api.PartitionableOperator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.PartitionableOperator.Partition;

public class OperatorPartitions {

  final DAG.OperatorWrapper operatorWrapper;

  public OperatorPartitions(DAG.OperatorWrapper operator) {
    this.operatorWrapper = operator;
  }

  private Map<DAG.InputPortMeta, List<Integer>> convertMapping(Map<InputPort<?>, List<Integer>> keys) {
    Map<DAG.InputPortMeta, List<Integer>> partitionKeys;
    partitionKeys = new HashMap<DAG.InputPortMeta, List<Integer>>(keys.size());
    Map<InputPort<?>, List<Integer>> partKeys = keys;
    for (Map.Entry<InputPort<?>, List<Integer>> portEntry : partKeys.entrySet()) {
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

    PartitionImpl(PartitionableOperator operator, Map<InputPort<?>, List<Integer>> partitionKeys) {
      this.operator = operator;
      this.partitionKeys = new PartitionPortMap();
      this.partitionKeys.putAll(partitionKeys);
      this.partitionKeys.modified = false;
    }

    PartitionImpl(PartitionableOperator operator) {
      this(operator, new PartitionPortMap());
    }

    @Override
    public Map<InputPort<?>, List<Integer>> getPartitionKeys() {
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


  public static class PartitionPortMap extends HashMap<InputPort<?>, List<Integer>>
  {
    private static final long serialVersionUID = 201212131624L;
    private boolean modified;

    private HashSet<Integer> validateNoRepeats(List<Integer> collection)
    {
      HashSet<Integer> hs = new HashSet<Integer>(collection.size());
      for (Integer bytes: collection) {
        hs.add(bytes);
      }

      if (hs.size() == collection.size()) {
        return hs;
      }

      return null;
    }

    private boolean validateEqual(List<Integer> collection1, List<Integer> collection2)
    {
      if (collection1 == null && collection2 == null) {
        return true;
      }

      if (collection1 == null || collection2 == null) {
        return false;
      }

      if (collection1.size() != collection2.size()) {
        return false;
      }

      HashSet<Integer> hs1 = validateNoRepeats(collection1);
      if (hs1 == null) {
        return false;
      }

      HashSet<Integer> hs2 = validateNoRepeats(collection2);
      if (hs2 == null) {
        return false;
      }

      for (Integer bb: hs1) {
        if (!hs2.contains(bb)) {
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
    public List<Integer> put(InputPort<?> key, List<Integer> value)
    {
      List<Integer> prev = super.put(key, value);
      if (!modified) {
        modified = !validateEqual(prev, value);
      }

      return prev;
    }

    @Override
    public void putAll(Map<? extends InputPort<?>, ? extends List<Integer>> m)
    {
      for (Map.Entry<? extends InputPort<?>, ? extends List<Integer>> entry: m.entrySet()) {
        put(entry.getKey(), entry.getValue());
      }
    }

    @Override
    @SuppressWarnings("element-type-mismatch")
    public List<Integer> remove(Object key)
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
