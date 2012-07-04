/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.policy;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.PhysicalNode;
import java.util.Set;

/**
 *
 * @author chetan
 */
public class RandomOne extends AbstractPolicy
{

  static final RandomOne instance = new RandomOne();

  public static RandomOne getInstance()
  {
    return instance;
  }

  private RandomOne()
  {
  }

  @Override
  public void distribute(Set<PhysicalNode> nodes, Data data)
  {
    int count = (int) (Math.random() * nodes.size());
    for (PhysicalNode node : nodes) {
      if (count-- == 0) {
        node.send(data);
        break;
      }
    }
  }
}
