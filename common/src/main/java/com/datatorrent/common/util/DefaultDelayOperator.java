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

/**
 * DefaultDelayOperator. This is the version of BaseDelayOperator that provides no data loss during recovery. It
 * incurs a run-time cost per tuple, and all tuples of the checkpoint window will be part of the checkpoint state.
 * Therefore if your application can tolerate data loss at recovery, BaseDelayOperator should be used instead.
 */
public class DefaultDelayOperator<T> extends BaseDelayOperator<T>
{
  protected List<T> lastWindowTuples = new ArrayList<>();

  @Override
  protected void processTuple(T tuple)
  {
    lastWindowTuples.add(tuple);
    super.processTuple(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    lastWindowTuples.clear();
  }

  @Override
  public void firstWindow(long windowId)
  {
    for (T tuple : lastWindowTuples) {
      output.emit(tuple);
    }
    super.firstWindow(windowId);
  }

}


