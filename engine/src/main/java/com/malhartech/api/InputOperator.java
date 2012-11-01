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
   * During normal operation, the input operators are given an opportunity to emit the tuples
   * under the control of the thread which controls the operator. The tuples which are emitted
   * during this call are sent out during the window with id equal to the argument.
   *
   */

  public void emitTuples();
}
