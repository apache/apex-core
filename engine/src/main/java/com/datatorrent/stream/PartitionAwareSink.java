/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stream;

import com.datatorrent.tuple.Tuple;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import java.util.Set;

/**
 *
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PartitionAwareSink<T> implements Sink<T>
{
  private final StreamCodec<T> serde;
  private final Set<Integer> partitions;
  private final int mask;
  private volatile Sink<T> output;
  private int count;

  /**
   *
   * @param serde
   * @param partitions
   * @param mask
   * @param output
   */
  public PartitionAwareSink(StreamCodec<T> serde, Set<Integer> partitions, int mask, Sink<T> output)
  {
    this.serde = serde;
    this.partitions = partitions;
    this.output = output;
    this.mask = mask;
  }

  /**
   *
   * @param payload
   */
  @Override
  public void put(T payload)
  {
    if (payload instanceof Tuple) {
      count++;
      output.put(payload);
    }
    else if (partitions.contains(serde.getPartition(payload) & mask)) {
      count++;
      output.put(payload);
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    }
    finally {
      if (reset) {
        count = 0;
      }
    }
  }

}
