/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.common;

import java.io.Serializable;

public class Pair<F, S> implements Serializable
{
  private static final long serialVersionUID = 731157267102567944L;
  public final F first;
  public final S second;

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 41 * hash + (this.first != null ? this.first.hashCode() : 0);
    hash = 41 * hash + (this.second != null ? this.second.hashCode() : 0);
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
    @SuppressWarnings("unchecked")
    final Pair<F, S> other = (Pair<F, S>)obj;
    if (this.first != other.first && (this.first == null || !this.first.equals(other.first))) {
      return false;
    }
    if (this.second != other.second && (this.second == null || !this.second.equals(other.second))) {
      return false;
    }
    return true;
  }

  public Pair(F first, S second)
  {
    this.first = first;
    this.second = second;
  }

  public F getFirst()
  {
    return first;
  }

  public S getSecond()
  {
    return second;
  }

  @Override
  public String toString()
  {
    return "[" + first + "," + second + "]";
  }

}