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
package com.datatorrent.common.util;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * DefaultDelayOperator. This is an implementation of the DelayOperator that has one input port and one output
 * port, and does a simple pass-through from the input port to the output port, while recording the tuples in memory
 * as checkpoint state.  Subclass of this operator can override this behavior by overriding processTuple(T tuple).
 *
 * Note that the engine automatically does a +1 on the output window ID since it is a DelayOperator.
 *
 * This DelayOperator provides no data loss during recovery, but it incurs a run-time cost per tuple, and all tuples
 * of the checkpoint window will be part of the checkpoint state.
 *
 * @since 3.3.0
 */
public class DefaultDelayOperator<T> extends BaseOperator implements Operator.DelayOperator
{
  public transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  public transient DefaultOutputPort<T> output = new DefaultOutputPort<>();

  protected List<T> lastWindowTuples = new ArrayList<>();

  protected void processTuple(T tuple)
  {
    lastWindowTuples.add(tuple);
    output.emit(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    lastWindowTuples.clear();
  }

  @Override
  public void firstWindow()
  {
    for (T tuple : lastWindowTuples) {
      output.emit(tuple);
    }
  }

}


