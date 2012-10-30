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
   * @param windowId Identifier of the window in the past.
   */
  public void replayEmitTuples(long windowId);

  /**
   * At the time when input operator emits the tuples on various of its output ports, it does
   * not know the window during which the tuples will be sent out. Through this call, the system
   * notifies the operator the windowId associated with the tuples. ackT
   *
   * @param windowId Identifier of the window during which the tuples were processed.
   * @param outputPort The port on which the tuples were emitted
   */
  public void postEmitTuples(long windowId, OutputPort<?> outputPort, Iterator<?> tuples);
}
