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
 * The base class for specifying partition policies, implements interface {@link com.datatorrent.bufferserver.policy.Policy}<p>
 * <br>
 *
 * @author chetan
 */
public class AbstractPolicy implements Policy
{

  /**
   *
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   */

  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
