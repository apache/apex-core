package com.datatorrent.stram.stream;

import java.util.HashSet;
import java.util.Set;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.plan.logical.StreamCodecWrapperForPersistance;

public class PartitionAwareSinkForPersistence extends PartitionAwareSink<Object>
{
  StreamCodecWrapperForPersistance<Object> serdeForPersistence;

  public PartitionAwareSinkForPersistence(StreamCodecWrapperForPersistance<Object> serde, Set<Integer> partitions, int mask, Sink<Object> output)
  {
    super(serde, partitions, mask, output);
    serdeForPersistence = serde;
  }

  public PartitionAwareSinkForPersistence(StreamCodecWrapperForPersistance<Object> serde, int mask, Sink<Object> output)
  {
    // If partition keys is null, everything should be passed to sink
    super(serde, createPartitionKeys(mask), mask, output);
    serdeForPersistence = serde;
  }

  private static Set<Integer> createPartitionKeys(int mask)
  {
    Set<Integer> partitions = new HashSet<Integer>();
    // Add all entries in mask to partitions keys
    for(int i =0 ; i <= mask; i++) {
      partitions.add(i);
    }
    return partitions;
  }

  @Override
  protected boolean canSendToOutput(Object payload)
  {
    if (!serdeForPersistence.shouldCaptureEvent(payload)) {
      return false;
    }

    return super.canSendToOutput(payload);
  }
}
