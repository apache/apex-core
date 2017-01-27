/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.BaseOperator;

/**
 *
 */
public class RecoverableInputOperator implements InputOperator, Operator.CheckpointNotificationListener
{
  public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();
  private long checkpointedWindowId;
  private transient boolean firstRun = true;
  private transient boolean first;
  private transient long windowId;
  private int maximumTuples = 20;
  private boolean simulateFailure;

  private static final Map<Long, Long> idMap = new HashMap<>();
  private static long tuple = 0;
  public static List<Long> emittedTuples = new ArrayList<>();

  public void setMaximumTuples(int count)
  {
    maximumTuples = count;
  }

  public static void initGenTuples()
  {
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
    logger.debug("{}", this);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    logger.debug("{}, windowId={}", this, Codec.getStringWindowId(windowId));
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

  @Override
  public void beforeCheckpoint(long windowId)
  {
    if (checkpointedWindowId == 0) {
      checkpointedWindowId = windowId;
    }
    logger.debug("{}, windowId={}", this, Codec.getStringWindowId(windowId));
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append("firstRun", this.firstRun)
            .append("checkpointedWindowId", Codec.getStringWindowId(checkpointedWindowId))
            .toString();
  }
}
