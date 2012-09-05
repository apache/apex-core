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
  private HashSet<byte[]> partitions;

  /**
   *
   * @param id
   */
  public BufferServerStreamContext(String id)
  {
    super(id);
  }

  /**
   *
   * @param partitionKeys
   */
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

  /**
   *
   * @return Collection<byte[]>
   */
  Collection<byte[]> getPartitions()
  {
    return partitions;
  }
}
