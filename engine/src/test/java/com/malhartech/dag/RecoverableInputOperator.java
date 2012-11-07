/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RecoverableInputOperator implements InputOperator
{
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>(this);
  transient boolean first;
  transient long windowId;
  boolean failed_once;
  int count;
  transient int fail;

  @Override
  public void emitTuples()
  {
    if (first) {
      output.emit((long)count);
      first = false;
      if (++count == 30) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    first = true;
  }

  @Override
  public void endWindow()
  {
    if (!failed_once) {
      failed_once = true;
      fail = 20;
    }

    if (fail > 0) {
      if (--fail == 0) {
        throw new RuntimeException("simulating failure");
      }
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }
}
