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
  transient boolean first;
  transient long windowId;
  int checkpointCount;
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
    if (checkpointCount > 0) {
      checkpointCount = 3;
    }

    logger.debug("checkpointCount = {}", checkpointCount);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
    checkpointCount++;
  }

  @Override
  public void committed(long windowId)
  {
    logger.debug("committed window {} and checkpoint {}", windowId, checkpointCount);
    if (checkpointCount == 2) {
      throw new RuntimeException("Failure Simulation from " + this);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RecoverableInputOperator.class);
}
