/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.util;

import java.io.Serializable;

public class Pair<F, S> implements Serializable {
  private static final long serialVersionUID = 731157267102567944L;

  private F first;
  private S second;

  public Pair(F first, S second) {
      this.first = first;
      this.second = second;
  }

  public void setFirst(F first) {
      this.first = first;
  }

  public void setSecond(S second) {
      this.second = second;
  }

  public F getFirst() {
      return first;
  }

  public S getSecond() {
      return second;
  }

  @Override
  public String toString() {
    return "[" + first + "," + second + "]";
  }

}