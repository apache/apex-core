/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.policy;

import com.malhartech.bufferserver.PhysicalNode;
import com.malhartech.bufferserver.util.SerializedData;
import java.util.Set;

/**
 *
 * @author chetan
 */
public class RoundRobin extends AbstractPolicy
{

  int index;

  public RoundRobin()
  {
    index = 0;
  }

  @Override
  public void distribute(Set<PhysicalNode> nodes, SerializedData data)
  {
    index %= nodes.size();
    int count = index++;
    for (PhysicalNode node : nodes) {
      if (count-- == 0) {
        node.send(data);
        break;
      }
    }
  }
}
