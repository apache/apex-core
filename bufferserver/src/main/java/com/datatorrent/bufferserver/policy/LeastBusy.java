/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.policy;

import java.util.Set;

import com.datatorrent.bufferserver.internal.PhysicalNode;
import com.datatorrent.common.util.SerializedData;

/**
 *
 * Implements load balancing by sending the tuple to the least busy partition<p>
 * <br>
 * Basic load balancing policy. Extends the base class {@link com.datatorrent.bufferserver.policy.AbstractPolicy}<br>
 *
 * @author chetan
 * @since 0.3.2
 */
public class LeastBusy extends AbstractPolicy
{
  static final LeastBusy instance = new LeastBusy();

  /**
   *
   * @return {@link com.datatorrent.bufferserver.policy.LeastBusy}
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
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   */
  @Override
  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    PhysicalNode theOne = null;

    for (PhysicalNode node: nodes) {
      if (theOne == null
              || node.getProcessedMessageCount() < theOne.getProcessedMessageCount()) {
        theOne = node;
      }
    }

    return theOne == null ? false : theOne.send(data);
  }

}
