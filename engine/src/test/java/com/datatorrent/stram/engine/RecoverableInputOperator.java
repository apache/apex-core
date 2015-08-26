/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.common.util.BaseOperator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.bufferserver.util.Codec;

/**
 *
 */
public class RecoverableInputOperator implements InputOperator, com.datatorrent.api.Operator.CheckpointListener
{
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>();
  private long checkpointedWindowId;
  boolean firstRun = true;
  transient boolean first;
  transient long windowId;
  int maximumTuples = 20;
  boolean simulateFailure;

  private static final Map<Long,Long> idMap = new HashMap<Long, Long>();
  private static long tuple = 0;
  public static List<Long> emittedTuples = new ArrayList<Long>();

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  public static void initGenTuples() {
    tuple = 0;
    idMap.clear();
    emittedTuples.clear();
  }

  @Override
  public void emitTuples()
  {
    if (first && maximumTuples > 0) {
      // only emit tuples once per window (first is reset to true in beginWindow())
      logger.debug("emitting {}", Codec.getStringWindowId(windowId));
      Long etuple = idMap.get(windowId);
      if (etuple == null) {
        etuple = tuple++;
        idMap.put(windowId, etuple);
      }
      output.emit(etuple);
      emittedTuples.add(etuple);
      first = false;
      maximumTuples--;
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
    firstRun = (checkpointedWindowId == 0);
    logger.debug("firstRun={} checkpointedWindowId={}", firstRun, Codec.getStringWindowId(checkpointedWindowId));
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
      logger.debug("firstRun={} checkpointedWindowId={}", firstRun, Codec.getStringWindowId(checkpointedWindowId));
    }

    logger.debug("{} checkpointed at {}", this, Codec.getStringWindowId(windowId));
  }

  @Override
  public void committed(long windowId)
  {
    logger.debug("{} committed at {} firstRun {}, checkpointedWindowId {}", this, Codec.getStringWindowId(windowId), firstRun, Codec.getStringWindowId(checkpointedWindowId));
    if (simulateFailure && firstRun && checkpointedWindowId > 0 && windowId > checkpointedWindowId) {
      throw new RuntimeException("Failure Simulation from " + this);
    }

    // we have emitted enough tuples and we have tested recovery, so we can shutdown once we have emitted enough.
    if (maximumTuples == 0) {
      BaseOperator.shutdown();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RecoverableInputOperator.class);

  void setSimulateFailure(boolean flag)
  {
    simulateFailure = flag;
  }
}
