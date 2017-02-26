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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

/**
 *
 * keep the information of one block
 *
 */
public class Block
{
  public static class OutOfBlockBufferMemoryException extends RuntimeException
  {
    private static final long serialVersionUID = 3813792889200989131L;
  }

  private static final Logger logger = LoggerFactory.getLogger(Block.class);

  public static final int DEFAULT_BLOCK_SIZE = 100000;

  //the capacity of the block
  private int capacity;

  /*
   * the size of the data.
   */
  private volatile int size;

  private int objectBeginOffset = 0;
  private byte[] buffer;

  /**
   * whether any slices have been exposed to the caller.
   */
  private boolean exposedSlices;

  private Block()
  {
    this(DEFAULT_BLOCK_SIZE);
  }

  public Block(int capacity)
  {
    if (capacity <= 0) {
      throw new IllegalArgumentException("Invalid capacity: " + capacity);
    }
    buffer = new byte[capacity];
    this.capacity = capacity;
  }

  public boolean write(byte data)
  {
    if (size + 1 > capacity) {
      if (exposedSlices) {
        return false;
      } else {
        reallocateBuffer(1);
      }
    }
    buffer[size++] = data;
    return true;
  }

  public boolean write(byte[] data)
  {
    return write(data, 0, data.length);
  }

  public boolean write(byte[] data, final int offset, final int length)
  {
    if (size + length > capacity) {
      if (exposedSlices) {
        return false;
      } else {
        reallocateBuffer(length);
      }
    }

    System.arraycopy(data, offset, buffer, size, length);
    size += length;
    return true;
  }

  public Slice reserve(int length)
  {
    if (size + length > capacity) {
      if (exposedSlices) {
        return null;
      } else {
        reallocateBuffer(length);
      }
    }

    Slice slice = new Slice(buffer, size, length);
    size += length;
    return slice;
  }


  /**
   * check the buffer size and reallocate if buffer is not enough
   *
   * @param length
   */
  private boolean reallocateBuffer(int length)
  {
    //calculate the new capacity
    capacity = size > length ? size * 2 : length * 2;

    byte[] oldBuffer = buffer;
    buffer = new byte[capacity];

    /**
     * no slices are exposed in this block yet (this is the first object in this block).
     * so we can reallocate and move the memory
     */
    if (size > 0) {
      System.arraycopy(oldBuffer, 0, buffer, 0, size);
    }
    return true;
  }

  /**
   * Similar to toSlice, this method is used to get the information of the
   * object regards the data already write to buffer. But unlike toSlice() which
   * indicates all the writes of this object are already done, this method can be called at
   * any time
   */
  public Slice getLastObjectSlice()
  {
    return new Slice(buffer, objectBeginOffset, size - objectBeginOffset);
  }

  public void discardLastObjectData()
  {
    if (objectBeginOffset == 0) {
      return;
    }
    size = objectBeginOffset;
  }

  public void moveLastObjectDataTo(Block newBlock)
  {
    if (size > objectBeginOffset) {
      newBlock.write(buffer, objectBeginOffset, size - objectBeginOffset);
      discardLastObjectData();
    }
  }

  /**
   * This method returns the slice that represents the serialized form.
   * The process of serializing an object should be one or multiple calls of write() followed by a toSlice() call.
   * A call to toSlice indicates the writes are done for this object
   *
   * @return
   */
  public BufferSlice toSlice()
  {
    if (size == objectBeginOffset) {
      throw new RuntimeException("data size is zero.");
    }
    BufferSlice slice = new BufferSlice(buffer, objectBeginOffset, size - objectBeginOffset);
    //prepare for next object
    objectBeginOffset = size;
    exposedSlices = true;
    return slice;
  }

  public void reset()
  {
    size = 0;
    objectBeginOffset = 0;
    exposedSlices = false;
  }

  /**
   * check if the block has enough space for the length
   *
   * @param length
   * @return
   */
  public boolean hasEnoughSpace(int length)
  {
    return size + length < capacity;
  }

  public long size()
  {
    return size;
  }

  public long capacity()
  {
    return capacity;
  }

  public boolean isFresh()
  {
    return (size == 0 && objectBeginOffset == 0 && exposedSlices == false);
  }

  /**
   * Returns whether the block is clear. The block is clear when there has not been any write calls since the last toSlice() call.
   *
   * @return
   */
  public boolean isClear()
  {
    return objectBeginOffset == size;
  }

  public void release()
  {
    reset();
    buffer = null;
  }
}
