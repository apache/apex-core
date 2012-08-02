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
public class LeastBusy extends AbstractPolicy
{

  static final LeastBusy instance = new LeastBusy();

  public static LeastBusy getInstance()
  {
    return instance;
  }

  private LeastBusy()
  {
  }

  @Override
  public void distribute(Set<PhysicalNode> nodes, SerializedData data)
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
