/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.policy;

import com.datatorrent.bufferserver.internal.PhysicalNode;
import com.datatorrent.bufferserver.util.SerializedData;

import java.util.Set;

/**
 *
 * Implements policy of giving a tuple to all nodes<p>
 * <br>
 * Is a broadcast policy. Extends the base class {@link com.datatorrent.bufferserver.policy.AbstractPolicy}<br>
 *
 * @author chetan
 * @since 0.3.2
 */
public class GiveAll extends AbstractPolicy
{
  final static GiveAll instance = new GiveAll();

  /**
   *
   * @return {@link com.datatorrent.bufferserver.policy.GiveAll}
   */
  public static GiveAll getInstance()
  {
    return instance;
  }

  /**
   *
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   * @return true if blocked, false otherwise
   * @throws InterruptedException
   */
  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    boolean retval = true;
    for (PhysicalNode node: nodes) {
      retval = node.send(data) & retval;
    }

    return retval;
  }

}
