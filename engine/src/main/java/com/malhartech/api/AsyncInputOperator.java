/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface AsyncInputOperator extends Operator
{
  /**
   * Emit the tuples on output ports so that they are part of the stream for the window identified by windowId.
   * @param windowId
   */
  public void emitTuples(long windowId);
}
