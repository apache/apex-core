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
package com.datatorrent.stram.webapp;

import com.datatorrent.common.util.NumberAggregate.DoubleAggregate;
import com.datatorrent.common.util.NumberAggregate.LongAggregate;

/**
 * <p>OperatorAggregationInfo class.</p>
 *
 * @since 1.0.2
 */
public class OperatorAggregationInfo
{
  public String name;
  public LongAggregate tuplesProcessedPSMA = new LongAggregate();
  public LongAggregate tuplesEmittedPSMA = new LongAggregate();
  public DoubleAggregate cpuPercentageMA = new DoubleAggregate();
  public LongAggregate latencyMA = new LongAggregate(true, false);
  public LongAggregate lastHeartbeat = new LongAggregate(true, true);
  public LongAggregate failureCount = new LongAggregate();
  public LongAggregate recoveryWindowId = new LongAggregate(true, true);
  public LongAggregate currentWindowId = new LongAggregate(true, true);
  public LongAggregate checkpointTime = new LongAggregate();

  public Object counters;
}
