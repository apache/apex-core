/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

/**
 * Input operators ie operators which do not consume payload from other operators but are able
 * to generate the payload for other operators must implement this interface.
 *
 * It's an error to provide input ports on operators which implement this interface since existence
 * of ports contradicts with the purpose of the interface.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface InputOperator extends Operator
{
  /**
   * The input operators are given an opportunity to emit the tuples which will be marked
   * as belonging to the windowId identified by immediately preceding beginWindow call.
   *
   * emitTuples can implement one or more tuples. Ideally it should emit as many tuples as
   * possible without going over the streaming window width boundary. Since operators can
   * easily spend too much time making that check, when not convenient they should emit
   * only as many tuples as they can confidently within the current window. The streaming
   * engine will make sure to call emitTuples multiple times within a giving streaming
   * window if it can. When it cannot, it will call endWindow.
   */
  public void emitTuples();

}
