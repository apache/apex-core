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
public class GiveAll extends AbstractPolicy
{
  final static GiveAll instance = new GiveAll();

  public static GiveAll getInstance()
  {
    return instance;
  }

  @Override
  public void distribute(Set<PhysicalNode> nodes, SerializedData data)
  {
    for (PhysicalNode node : nodes) {
      node.send(data);
    }
  }
}
