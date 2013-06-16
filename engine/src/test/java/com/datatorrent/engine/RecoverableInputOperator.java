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
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>(this);
  long checkpointedWindowId;
  boolean firstRun = true;
  transient boolean first;
  transient long windowId;
  int maximumTuples = 20;

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  @Override
  public void emitTuples()
  {
    if (first) {
      output.emit(windowId);
      first = false;
      if (--maximumTuples == 0) {
        throw new RuntimeException(new InterruptedException("Done!"));
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
    firstRun &= checkpointedWindowId == 0;
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    if (checkpointedWindowId == 0) {
      checkpointedWindowId = windowId;
    }
  }

  @Override
  public void committed(long windowId)
  {
    if (firstRun && checkpointedWindowId > 0 && windowId > checkpointedWindowId) {
      throw new RuntimeException("Failure Simulation from " + this);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RecoverableInputOperator.class);
}
