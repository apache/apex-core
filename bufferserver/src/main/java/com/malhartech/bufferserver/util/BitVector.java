/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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

}
