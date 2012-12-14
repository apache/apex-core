/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.CheckpointListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class RecoverableInputOperator implements InputOperator, CheckpointListener
{
  private static final Logger logger = LoggerFactory.getLogger(RecoverableInputOperator.class);
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>(this);
  transient boolean first;
  transient long windowId;
  boolean failed;
  transient boolean transient_fail;
  int maximumTuples = 20;

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  @Override
  public void emitTuples()
  {
    if (first) {
//      logger.debug("generating tuple {}", Codec.getStringWindowId(windowId));
      output.emit(windowId);
      first = false;
      if (--maximumTuples == 0) {
        throw new RuntimeException(new InterruptedException("Just want to stop!"));
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
  }

  @Override
  public void setup(OperatorContext context)
  {
    transient_fail = !failed;
    failed = true;
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    if (transient_fail) {
      throw new RuntimeException("Failure Simulation from " + this);
    }
  }

  @Override
  public void committed(long windowId)
  {
  }
}
