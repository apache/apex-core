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
 * Implements policy of giving a tuple to all nodes<p>
 * <br>
 * Is a broadcast policy. Extends the base class {@link com.malhartech.bufferserver.policy.AbstractPolicy}<br>
 *
 * @author chetan
 */
public class GiveAll extends AbstractPolicy
{
  final static GiveAll instance = new GiveAll();

  /**
   *
   * @return {@link com.malhartech.bufferserver.policy.GiveAll}
   */
  public static GiveAll getInstance()
  {
    return instance;
  }

  /**
   *
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send
   * @return true if blocked, false otherwise
   */
  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    boolean retval = false;
    for (PhysicalNode node : nodes) {
      if (node.send(data)) {
        retval = true;
      }
    }

    return retval;
  }
}
