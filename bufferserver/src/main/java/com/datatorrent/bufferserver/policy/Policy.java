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
 * The base interface for implementing/specifying partition policies<p>
 * <br>
 *
 * @author chetan
 */
public interface Policy
{
  /**
   *
   *
   * @param nodes Set of downstream {@link com.datatorrent.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.datatorrent.bufferserver.util.SerializedData} to be send
   * @throws InterruptedException
   */

  public boolean distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException;

}
