/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.common.util;

import java.util.Arrays;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Slice
{
  public byte[] buffer;
  public int offset;
  public int length;

  public Slice(byte[] array, int offset, int length)
  {
    buffer = array;
    this.offset = offset;
    this.length = length;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 59 * hash + Arrays.hashCode(this.buffer);
    hash = 59 * hash + this.offset;
    hash = 59 * hash + this.length;
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
    final Slice other = (Slice)obj;
    if (!Arrays.equals(this.buffer, other.buffer)) {
      return false;
    }
    if (this.offset != other.offset) {
      return false;
    }
    if (this.length != other.length) {
      return false;
    }
    return true;
  }

  @Override
  public String toString()
  {
    return "Slice{" + (length > 256 ? "buffer=" + buffer + ", offset=" + offset + ", length=" + length : Arrays.toString(Arrays.copyOfRange(buffer, offset, offset + length))) + '}';
  }

}
