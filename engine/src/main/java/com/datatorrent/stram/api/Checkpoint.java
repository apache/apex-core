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
package com.datatorrent.stram.api;

import java.util.Comparator;

import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.bufferserver.util.Codec;

/**
 * <p>Checkpoint class.</p>
 *
 * @since 0.9.4
 */
public class Checkpoint implements com.datatorrent.api.Stats.Checkpoint
{
  /**
   * WindowId used to store the state of the operator which has not processed a single tuple.
   */
  public final long windowId;
  public final int applicationWindowCount;
  public final int checkpointWindowCount;

  public Checkpoint(long windowId, int appCount, int checkpointCount)
  {
    this.windowId = windowId;
    this.applicationWindowCount = appCount;
    this.checkpointWindowCount = checkpointCount;
  }

  @Override
  public String toString()
  {
    return '{' + Codec.getStringWindowId(windowId) + ", " + applicationWindowCount + ", " + checkpointWindowCount + '}';
  }

  public static Checkpoint min(Checkpoint first, Checkpoint second)
  {
    if (first.windowId < second.windowId) {
      return first;
    }
    return second;
  }

  public static Checkpoint max(Checkpoint first, Checkpoint second)
  {
    if (first.windowId < second.windowId) {
      return second;
    }
    return first;
  }

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 29 * hash + (int)(this.windowId ^ (this.windowId >>> 32));
    hash = 29 * hash + this.applicationWindowCount;
    hash = 29 * hash + this.checkpointWindowCount;
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
    final Checkpoint other = (Checkpoint)obj;
    if (this.windowId != other.windowId) {
      return false;
    }
    if (this.applicationWindowCount != other.applicationWindowCount) {
      return false;
    }
    if (this.checkpointWindowCount != other.checkpointWindowCount) {
      return false;
    }
    return true;
  }

  @Override
  public long getWindowId()
  {
    return windowId;
  }

  public static class CheckpointComparator implements Comparator<Checkpoint>
  {
    @Override
    public int compare(Checkpoint o1, Checkpoint o2)
    {
      return Long.compare(o1.windowId, o2.windowId);
    }
  }

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  public static final Checkpoint INITIAL_CHECKPOINT = new Checkpoint(Stateless.WINDOW_ID, 0, 0);
  private static final long serialVersionUID = 201402152116L;
}
