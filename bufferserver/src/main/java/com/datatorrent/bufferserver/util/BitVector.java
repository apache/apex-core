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
package com.datatorrent.bufferserver.util;

/**
 * <p>BitVector class.</p>
 *
 * @since 0.3.2
 */
public class BitVector
{
  final int mask;
  final int bits;

  public BitVector(int bits, int mask)
  {
    this.mask = mask;
    this.bits = bits & mask;
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 37 * hash + this.mask;
    hash = 37 * hash + this.bits;
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final BitVector other = (BitVector)obj;
    if (this.mask != other.mask) {
      return false;
    }
    if (this.bits != other.bits) {
      return false;
    }
    return true;
  }

  public boolean matches(int value)
  {
    return (value & mask) == bits;
  }

  @Override
  public String toString()
  {
    return "BitVector{" + "mask=" + Integer.toBinaryString(mask) + ", bits=" + Integer.toBinaryString(bits) + '}';
  }

}
