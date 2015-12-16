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

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * SimpleDelayOperator. This is an implementation of the DelayOperator that has one input port and one output
 * port, and does a simple pass-through from the input port to the output port.  Subclass of this operator can
 * override this behavior by overriding processTuple(T tuple).
 *
 * Since the firstWindow method does not do anything, using this operator as-is means data loss during recovery.  In
 * order to achieve zero data loss during recovery, implementations must persist relevant tuples before the recovery
 * checkpoint for emitting during the first window after recovery by providing an implementation of the firstWindow
 * method.
 *
 * Note that the engine automatically does a +1 on the output window ID since it is a DelayOperator.
 */
public class SimpleDelayOperator<T> extends BaseOperator implements Operator.DelayOperator
{
  public transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  public transient DefaultOutputPort<T> output = new DefaultOutputPort<T>();

  protected void processTuple(T tuple)
  {
    output.emit(tuple);
  }

  @Override
  public void firstWindow(long windowId)
  {
  }

}
