/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.policy;

import com.malhartech.bufferserver.internal.PhysicalNode;
import com.malhartech.bufferserver.util.SerializedData;
import java.util.Set;

/**
 *
 * Randomly distributes tuples to downstream nodes. A random load balancing policy<p>
 * <br>
 * A generic random load balancing policy. Extends the base class {@link com.malhartech.bufferserver.policy.AbstractPolicy}<br>
 *
 * @author chetan
 */
public class RandomOne extends AbstractPolicy
{
  static final RandomOne instance = new RandomOne();

  /**
   *
   * @return {@link com.malhartech.bufferserver.policy.RandomOne}
   */
  public static RandomOne getInstance()
  {
    return instance;
  }

  /**
   * Constructor
   */
  private RandomOne()
  {
  }

  /**
   *
   *
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send
   */

  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    int count = (int)(Math.random() * nodes.size());
    /*
     * Should look at accessing nodes within the Set as array. Will save iteration through all the
     * physical nodes.
     *
     */
    for (PhysicalNode node : nodes) {
      if (count-- == 0) {
        return node.send(data);
      }
    }

    return false;
  }

}
