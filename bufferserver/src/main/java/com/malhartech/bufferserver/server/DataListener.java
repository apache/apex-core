/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.util.BitVector;
import java.util.Collection;

/**
 *
 * Waits for data to be added to the buffer server and then acts on it<p>
 * <br>
 * The behavior upon data addition is customizable
 *
 * @author chetan
 */
public interface DataListener
{
  public static final BitVector NULL_PARTITION = new BitVector(0,0);

  /**
   *
   * @param partition
   */
  public void dataAdded();

  /**
   *
   * @param partitions
   * @return int
   */
  public int getPartitions(Collection<BitVector> partitions);
}
