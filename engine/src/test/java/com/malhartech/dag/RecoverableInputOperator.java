/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RecoverableInputOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(RecoverableInputOperator.class);
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>(this);
  transient boolean first;
  transient long windowId;
  boolean failed_once;
  int maximumTuples = 20;
  transient int fail;

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  @Override
  public void emitTuples()
  {
    if (first) {
//      logger.debug("generating tuple {}", Long.toHexString(windowId));
      output.emit(windowId);
      first = false;
      if (--maximumTuples == 0) {
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
      fail = maximumTuples >> 1;
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
    if (maximumTuples < 4) {
      throw new RuntimeException("MaximumTuples value should at least be 4 for this Operator to serve its purpose!");
    }
  }

  @Override
  public void teardown()
  {
  }
}
