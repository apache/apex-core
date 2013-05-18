/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputOperator extends Operator
{
  /**
   * The input operators are given an opportunity to emit the tuples which will be marked
   * as belonging to the windowId identified by immediately preceding beginWindow call.
   *
   */
  public void emitTuples();
}
