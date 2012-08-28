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
 * The base class for specifying partition policies, implements interface {@link com.malhartech.bufferserver.policy.Policy}<p>
 * <br>
 * 
 * @author chetan
 */
public class AbstractPolicy implements Policy
{

 /**
   * 
   * @param nodes Set of downstream {@link com.malhartech.bufferserver.PhysicalNode}s
   * @param data Opaque {@link com.malhartech.bufferserver.util.SerializedData} to be send
   */
  public void distribute(Set<PhysicalNode> nodes, SerializedData data)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
