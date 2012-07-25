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
public class BufferServerStreamContext extends StreamContext
{
  private String sourceId;
  private String sinkId;
  private String id;
  private HashSet<byte[]> partitions;

  public String getSourceId()
  {
    return sourceId;
  }

  public String getSinkId()
  {
    return sinkId;
  }

  public void setSourceId(String id)
  {
    sourceId = id;
  }

  public void setSinkId(String id)
  {
    sinkId = id;
  }
  
  public void addPartition()
  {
    
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
