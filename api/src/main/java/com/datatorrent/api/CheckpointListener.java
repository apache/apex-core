/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

/**
 * Operators must implement this interface if they are interested in being notified as
 * soon as the operator state is checkpointed or committed.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface CheckpointListener
{
  public void checkpointed(long windowId);

  public void committed(long windowId);

}
