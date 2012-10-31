/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.api;

import java.util.Iterator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputOperator extends Operator
{
  /**
   * During the failure recovery, the input operators are given a chance to replay the tuples
   * that they emited in the past. The association of tuples and windowIds are notified to the
   * operator through ack calls.
   *
   * @param windowId Identifier of the window in the past during which tuples will be assumed emitted.
   */
  public void replayTuples(long windowId);

  /**
   * During normal operation, the input operators are given an opportunity to emit the tuples
   * under the control of the thread which controls the operator. The tuples which are emitted
   * during this call are sent out during the window with id equal to the argument.
   *
   * @param windowId Identifier of the current window in which tuples will be emitted.
   */
  public void emitTuples(long windowId);
}
