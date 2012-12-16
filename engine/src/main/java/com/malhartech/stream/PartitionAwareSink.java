/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.Tuple;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PartitionAwareSink<T> implements Sink<T>
{
  private final StreamCodec<T> serde;
  private final HashSet<Integer> partitions;
  private volatile Sink<T> output;

  /**
   *
   * @param serde
   * @param partitions
   * @param output
   */
  public PartitionAwareSink(StreamCodec<T> serde, List<Integer> partitions, Sink<T> output)
  {
    this.serde = serde;

    this.partitions = new HashSet<Integer>(partitions.size());
    for (Integer partition: partitions) {
      this.partitions.add(partition);
    }

    this.output = output;
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
    else if (partitions.contains(serde.getPartition(payload))) {
      output.process(payload);
    }
  }
}
