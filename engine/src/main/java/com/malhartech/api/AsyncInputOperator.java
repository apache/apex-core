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
   * Called by the container to allow the operator to emit collected input to the output port(s).
   * @param windowId
   */
  public void injectTuples(long windowId);
}
