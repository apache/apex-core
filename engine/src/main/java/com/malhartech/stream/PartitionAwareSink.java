/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.Tuple;
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
  public void process(T payload)
  {
    if (payload instanceof Tuple) {
      output.process(payload);
    }
    else if (partitions.contains(serde.getPartition(payload) & mask)) {
      output.process(payload);
    }
  }

}
