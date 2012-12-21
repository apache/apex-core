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
 * Distributes to downstream nodes in a roundrobin fashion. A round robin load balancing policy<p>
 * <br>
 * A round robin load balaning policy. Does not take into account busy/load of a downstream physical node. Extends the base class {@link com.malhartech.bufferserver.policy.AbstractPolicy}<br>
 * <br>
 * @author chetan
 */
public class RoundRobin extends AbstractPolicy
{

  int index;

  /**
   * Constructor
   */
  public RoundRobin()
  {
    index = 0;
  }

  /**
   * 
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send 
   */
  @Override
  public void distribute(Set<PhysicalNode> nodes, SerializedData data)
  {
    index %= nodes.size();
    int count = index++;
    /*
     * May need to look at accessing nodes as arrays, so that iteration can be avoided
     * This matters if say there are 1000+ partitions(?) and this may happen in a Big Message
     * application
     * 
     */
    for (PhysicalNode node : nodes) {
      if (count-- == 0) {
        node.send(data);
        break;
      }
    }
  }
}
