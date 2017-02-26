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

import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * This is a stream which manages blocks and supports window related operations.
 *
 */
public class WindowedBlockStream extends BlockStream implements WindowListener, WindowCompleteListener
{
  private static final Logger logger = LoggerFactory.getLogger(WindowedBlockStream.class);
  /**
   * Map from windowId to blockIds
   */
  protected SetMultimap<Long, Integer> windowToBlockIds = HashMultimap.create();

  /**
   * set of all free blockIds.
   */
  protected Set<Integer> freeBlockIds = Sets.newHashSet();

  // max block index; must be >= 0
  protected int maxBlockIndex = 0;

  //init current window id to invalid number
  protected long currentWindowId = -1;

  protected BlockReleaseStrategy releaseStrategy = new DefaultBlockReleaseStrategy();

  public WindowedBlockStream()
  {
    super();
  }

  public WindowedBlockStream(int blockCapacity)
  {
    super(blockCapacity);
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    moveToNextWindow();
  }

  /**
   * make sure different windows will not share any blocks. Move to next block if
   * current block is already used.
   */
  protected void moveToNextWindow()
  {
    //use current block if it hasn't be used, else, move to next block
    Block block = getOrCreateCurrentBlock();
    if (!block.isClear()) {
      throw new RuntimeException("Current block not clear, should NOT move to next window. Please call toSlice() to output data first");
    }
    if (block.size() > 0) {
      moveToNextBlock();
    }
    windowToBlockIds.put(currentWindowId, currentBlockIndex);
  }

  /**
   * This method tries to use a free block first. Allocate a new block if there
   * are no free blocks
   *
   * @return The previous block
   */
  @Override
  protected Block moveToNextBlock()
  {
    Block previousBlock = currentBlock;
    if (!freeBlockIds.isEmpty()) {
      currentBlockIndex = freeBlockIds.iterator().next();
      freeBlockIds.remove(currentBlockIndex);
      currentBlock = this.blocks.get(currentBlockIndex);
      if (!currentBlock.isFresh()) {
        throw new RuntimeException("Assigned non fresh block.");
      }
    } else {
      currentBlockIndex = ++maxBlockIndex;
      currentBlock = getOrCreateCurrentBlock();
      if (!currentBlock.isFresh()) {
        throw new RuntimeException("Assigned non fresh block.");
      }
    }
    if (currentWindowId >= 0) {
      windowToBlockIds.put(currentWindowId, currentBlockIndex);
    }
    return previousBlock;
  }

  @Override
  public void endWindow()
  {
    windowToBlockIds.removeAll(currentBlock);
    releaseMemory();
  }

  @Override
  public void completeWindow(long windowId)
  {
    Set<Long> windIds = Sets.newHashSet(windowToBlockIds.keySet());
    for (long windId : windIds) {
      if (windId <= windowId) {
        resetWindow(windId);
      }
    }
  }

  protected void resetWindow(long windowId)
  {
    Set<Integer> removedBlockIds = windowToBlockIds.removeAll(windowId);

    int removedSize = 0;
    for (int blockId : removedBlockIds) {
      removedSize += blocks.get(blockId).size();
      Block theBlock = blocks.get(blockId);
      theBlock.reset();
      if (theBlock == currentBlock) {
        //the client code could ask reset up to current window
        //but the reset block should not be current block. current block should be reassigned.
        moveToNextBlock();
      }
      logger.debug("reset block: {}, currentBlock: {}", blockId, theBlock);
    }

    freeBlockIds.addAll(removedBlockIds);
    size -= removedSize;
  }

  @Override
  public void reset()
  {
    super.reset();

    windowToBlockIds.clear();
    if (blocks.size() > 1) {
      //all blocks are free now except the current one
      freeBlockIds.addAll(blocks.keySet());
      freeBlockIds.remove(currentBlockIndex);
    }
  }

  /**
   * The size of the data of all windows with id less than or equals to windowId
   * @param windowId
   * @return
   */
  public long dataSizeUpToWindow(long windowId)
  {
    long totalSize = 0;
    for (long winId : windowToBlockIds.keySet()) {
      totalSize += dataSizeOfWindow(winId);
    }
    return totalSize;
  }

  protected long dataSizeOfWindow(long windowId)
  {
    long sizeOfWindow = 0;
    Set<Integer> blockIds = windowToBlockIds.get(windowId);
    if (blockIds != null) {
      for (int blockId : blockIds) {
        sizeOfWindow += blocks.get(blockId).size();
      }
    }
    return sizeOfWindow;
  }

  public void releaseMemory()
  {
    /**
     * report and release extra blocks
     */
    releaseStrategy.currentFreeBlocks(freeBlockIds.size());
    int releasingBlocks = Math.min(releaseStrategy.getNumBlocksToRelease(), freeBlockIds.size());
    int releasedBlocks = 0;
    Iterator<Integer> iter = freeBlockIds.iterator();
    while (releasedBlocks < releasingBlocks) {
      //release blocks
      int blockId = iter.next();
      iter.remove();
      blocks.remove(blockId);
      releasedBlocks++;
    }

    /**
     * report number of released blocks
     */
    if (releasedBlocks > 0) {
      releaseStrategy.releasedBlocks(releasedBlocks);
    }
  }

  /**
   * This method releases all free memory immediately.
   * This method will not be controlled by release strategy
   */
  public void releaseAllFreeMemory()
  {
    int releasedBlocks = 0;

    Iterator<Integer> iter = freeBlockIds.iterator();
    while (iter.hasNext()) {
      //release blocks
      int blockId = iter.next();
      iter.remove();
      blocks.remove(blockId);
      releasedBlocks++;
    }

    /**
     * report number of released blocks
     */
    if (releasedBlocks > 0) {
      releaseStrategy.releasedBlocks(releasedBlocks);
    }
  }
}
