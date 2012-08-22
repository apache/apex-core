/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.StreamContext;
import java.util.Collection;
import java.util.HashSet;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */

/**
 * 
 * Implements stream context for buffer server<p>
 * <br>
 * Extends StreamContext and defines upstream and downstream nodes<br>
 * Provides wiring for the stream<br>
 * Manages partitioning<br>
 * <br>
 */

public class BufferServerStreamContext extends StreamContext
{
  private String id;
  private HashSet<byte[]> partitions;

  public BufferServerStreamContext(String upstreamNodeId, String downstreamNodeId)
  {
    super(upstreamNodeId, downstreamNodeId);
  }

  public void setId(String id)
  {
    this.id = id;
  }
  
  public String getId()
  {
    return id;
  }

  public void setPartitions(Collection<byte[]> partitionKeys)
  {
    if (partitionKeys == null) {
      partitions = null;
    }
    else {
      partitions = new HashSet<byte[]>(partitionKeys.size());
      partitions.addAll(partitionKeys);
    }
  }

  Collection<byte[]> getPartitions()
  {
    return partitions;
  }
}
