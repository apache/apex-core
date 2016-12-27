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

import org.getopt.util.hash.MurmurHash;

import org.apache.commons.lang3.ArrayUtils;

import com.datatorrent.netlet.util.Slice;

/**
 * com.datatorrent.netlet.util.Slice has problem with the hashCode(), so
 * override here
 *
 */
public class BufferSlice extends Slice
{
  private static final long serialVersionUID = -471209532589983329L;
  public static final BufferSlice EMPTY_SLICE = new BufferSlice(ArrayUtils.EMPTY_BYTE_ARRAY);

  //for kyro
  private BufferSlice()
  {
    //the super class's default constructor is private and can't called.
    super(null, 0, 0);
  }

  public BufferSlice(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  public BufferSlice(byte[] array)
  {
    super(array);
  }

  public BufferSlice(Slice netletSlice)
  {
    this(netletSlice.buffer, netletSlice.offset, netletSlice.length);
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 59 * hash + MurmurHash.hash(buffer, hash, offset, length);
    hash = 59 * hash + this.length;
    return hash;
  }

  /**
   * let this class equals with com.datatorrent.netlet.util.Slice
   */
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (!Slice.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    final Slice other = (Slice)obj;
    if (this.length != other.length) {
      return false;
    }

    final int offset1 = this.offset;
    final byte[] buffer1 = this.buffer;
    int i = offset1 + this.length;

    final byte[] buffer2 = other.buffer;
    int j = other.offset + other.length;

    while (i-- > offset1) {
      if (buffer1[i] != buffer2[--j]) {
        return false;
      }
    }

    return true;
  }
}
