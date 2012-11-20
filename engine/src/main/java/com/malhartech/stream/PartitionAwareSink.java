/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.engine.Tuple;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PartitionAwareSink<T> implements Sink<T>
{
  private final StreamCodec<T> serde;
  private final HashSet<ByteBuffer> partitions;
  private volatile Sink<T> output;

  /**
   *
   * @param serde
   * @param partitions
   * @param output
   */
  public PartitionAwareSink(StreamCodec<T> serde, List<byte[]> partitions, Sink<T> output)
  {
    this.serde = serde;

    this.partitions = new HashSet<ByteBuffer>(partitions.size());
    for (byte[] partition: partitions) {
      this.partitions.add(ByteBuffer.wrap(partition));
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
    else if (partitions.contains(ByteBuffer.wrap(serde.getPartition(payload)))) {
      output.process(payload);
    }
  }
}
