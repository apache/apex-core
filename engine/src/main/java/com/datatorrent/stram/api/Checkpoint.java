/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 */
package com.datatorrent.stram.api;

import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.bufferserver.util.Codec;

/**
 * <p>Checkpoint class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
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

  @SuppressWarnings("FieldNameHidesFieldInSuperclass")
  public static final Checkpoint INITIAL_CHECKPOINT = new Checkpoint(Stateless.WINDOW_ID, 0, 0);
  private static final long serialVersionUID = 201402152116L;
}
