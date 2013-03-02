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
 * Implements load balancing by sending the tuple to the least busy partition<p>
 * <br>
 * Basic load balancing policy. Extends the base class {@link com.malhartech.bufferserver.policy.AbstractPolicy}<br>
 *
 * @author chetan
 */
public class LeastBusy extends AbstractPolicy
{
  static final LeastBusy instance = new LeastBusy();

  /**
   *
   * @return {@link com.malhartech.bufferserver.policy.LeastBusy}
   */
  public static LeastBusy getInstance()
  {
    return instance;
  }

  /**
   * Constructor
   */
  private LeastBusy()
  {
  }

  /**
   *
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send
   *
   */
  @Override
  public void distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    PhysicalNode theOne = null;

    for (PhysicalNode node : nodes) {
      if (theOne == null
              || node.getProcessedMessageCount() < theOne.getProcessedMessageCount()) {
        theOne = node;
      }
    }

    if (theOne != null) {
      theOne.send(data);
    }
  }

}
