/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.PartitionableOperator;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Partition extends HashMap<InputPort, List<byte[]>> implements com.malhartech.api.PartitionableOperator.Partition, Cloneable
{
  private int load;
  private PartitionableOperator operator;
  private boolean modified;

  private HashSet<ByteBuffer> validateNoRepeats(List<byte[]> collection)
  {
    HashSet<ByteBuffer> hs = new HashSet<ByteBuffer>(collection.size());
    for (byte[] bytes: collection) {
      hs.add(ByteBuffer.wrap(bytes));
    }

    if (hs.size() == collection.size()) {
      return hs;
    }

    return null;
  }

  private boolean validateEqual(List<byte[]> collection1, List<byte[]> collection2)
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

    HashSet<ByteBuffer> hs1 = validateNoRepeats(collection1);
    if (hs1 == null) {
      return false;
    }

    HashSet<ByteBuffer> hs2 = validateNoRepeats(collection2);
    if (hs2 == null) {
      return false;
    }

    for (ByteBuffer bb: hs1) {
      if (!hs2.contains(bb)) {
        return false;
      }
    }

    return true;
  }

  public void setLoad(int load)
  {
    this.load = load;
  }

  @Override
  public int getLoad()
  {
    return load;
  }

  @Override
  public PartitionableOperator getOperator()
  {
    return operator;
  }

  @Override
  public void setOperator(PartitionableOperator operator)
  {
    if (operator != this.operator) {
      modified = true;
      this.operator = operator;
    }
  }

  public boolean isModified()
  {
    return modified;
  }

  @Override
  public List<byte[]> put(InputPort key, List<byte[]> value)
  {
    List<byte[]> prev = super.put(key, value);
    if (!modified) {
      modified = !validateEqual(prev, value);
    }

    return prev;
  }

  @Override
  public void putAll(Map<? extends InputPort, ? extends List<byte[]>> m)
  {
    for (Entry<? extends InputPort, ? extends List<byte[]>> entry: m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  @SuppressWarnings("element-type-mismatch")
  public List<byte[]> remove(Object key)
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

  @Override
  public PartitionableOperator.Partition getInstance()
  {
    return new Partition();
  }
}
