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
package com.datatorrent.common.util;

import java.io.Serializable;

/**
 * <p>Pair class.</p>
 *
 * @since 0.3.2
 */
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
