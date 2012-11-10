/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.PartitionableOperator;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Partition extends HashMap<InputPort, Set<byte[]>> implements com.malhartech.api.PartitionableOperator.Partition
{
  private int load;
  private PartitionableOperator operator;
  private boolean modified;

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
  public Set<byte[]> put(InputPort key, Set<byte[]> value)
  {
    Set<byte[]> prev = super.put(key, value);
    if (value != prev) {
      modified = true;
    }
    return prev;
  }

  @Override
  public void putAll(Map<? extends InputPort, ? extends Set<byte[]>> m)
  {
    if (!m.isEmpty()) {
      super.putAll(m);
      modified = true;
    }
  }

  @Override
  @SuppressWarnings("element-type-mismatch")
  public Set<byte[]> remove(Object key)
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
