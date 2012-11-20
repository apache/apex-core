/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 *
 * Waits for data to be added to the buffer server and then acts on it<p>
 * <br>
 * The behavior upon data addition is customizable
 * @author chetan
 */
public interface DataListener
{
  public static final ByteBuffer NULL_PARTITION = ByteBuffer.allocate(0);

  public void setBaseSeconds(int baseSeconds);
  /**
   *
   * @param partition
   */
  public void dataAdded(ByteBuffer partition);

  /**
   *
   * @param partitions
   * @return int
   */
  public int getPartitions(Collection<ByteBuffer> partitions);
}
