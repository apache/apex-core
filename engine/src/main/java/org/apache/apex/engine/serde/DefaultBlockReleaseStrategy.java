/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.engine.serde;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * This implementation get the minimum number of free blocks in the period to release.
 *
 */
public class DefaultBlockReleaseStrategy implements BlockReleaseStrategy
{
  public static final int DEFAULT_PERIOD = 60; // 60 reports
  private CircularFifoBuffer freeBlockNumQueue;

  public DefaultBlockReleaseStrategy()
  {
    this(DEFAULT_PERIOD);
  }

  public DefaultBlockReleaseStrategy(int period)
  {
    freeBlockNumQueue = new CircularFifoBuffer(period);
  }

  /**
   * The stream calls this to report to the strategy how many blocks are free currently.
   * @param freeBlockNum
   */
  @Override
  public void currentFreeBlocks(int freeBlockNum)
  {
    if (freeBlockNum < 0) {
      throw new IllegalArgumentException("The number of free blocks could not less than zero.");
    }
    freeBlockNumQueue.add(new MutableInt(freeBlockNum));
  }

  /**
   * Get how many blocks that can be released
   * @return
   */
  @Override
  public int getNumBlocksToRelease()
  {
    int minNum = Integer.MAX_VALUE;
    for (Object num : freeBlockNumQueue) {
      minNum = Math.min(((MutableInt)num).intValue(), minNum);
    }
    return minNum;
  }


  /**
   * report how many blocks that have been released.
   * @param numReleasedBlocks
   */
  @Override
  public void releasedBlocks(int numReleasedBlocks)
  {
    if (numReleasedBlocks == 0) {
      return;
    }
    if (numReleasedBlocks < 0) {
      throw new IllegalArgumentException("Num of released blocks should not be negative");
    }
    /**
     * decrease by released blocks
     */
    for (Object num : freeBlockNumQueue) {
      ((MutableInt)num).setValue(Math.max(((MutableInt)num).intValue() - numReleasedBlocks, 0));
    }
  }

}
