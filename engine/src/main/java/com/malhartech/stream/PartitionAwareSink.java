/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.engine.SerDe;
import com.malhartech.engine.Tuple;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PartitionAwareSink implements Sink
{
  private final SerDe serde;
  private final HashSet<ByteBuffer> partitions;
  private volatile Sink output;

  /**
   *
   * @param serde
   * @param partitions
   * @param output
   */
  public PartitionAwareSink(SerDe serde, List<byte[]> partitions, Sink output)
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
  public void process(Object payload)
  {
    if (payload instanceof Tuple) {
      output.process(payload);
    }
    else if (partitions.contains(ByteBuffer.wrap(serde.getPartition(payload)))) {
      output.process(payload);
    }
  }
}
