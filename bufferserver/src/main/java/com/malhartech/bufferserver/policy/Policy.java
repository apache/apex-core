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
 * The base interface for implementing/specifying partition policies<p>
 * <br>
 *
 * @author chetan
 */
public interface Policy
{
  /**
   *
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send
   * @throws InterruptedException 
   */
  public void distribute(Set<PhysicalNode> nodes, SerializedData data) throws InterruptedException;

}
