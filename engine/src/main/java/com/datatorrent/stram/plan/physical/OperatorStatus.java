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
package com.datatorrent.stram.plan.physical;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.datatorrent.api.Stats;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;

import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.StatsRevisions.VersionedLong;
import com.datatorrent.stram.util.MovingAverage.MovingAverageLong;
import com.datatorrent.stram.util.MovingAverage.TimedMovingAverageLong;

/**
 * <p>OperatorStatus class.</p>
 *
 * @since 0.9.1
 */
public class OperatorStatus implements BatchedOperatorStats, java.io.Serializable
{
  private static final long serialVersionUID = 201312231552L;

  public class PortStatus
  {
    public String portName;
    public long totalTuples;
    public String recordingId;
    public final TimedMovingAverageLong tuplesPMSMA;
    public final TimedMovingAverageLong bufferServerBytesPMSMA;
    public final MovingAverageLong queueSizeMA;

    public PortStatus()
    {
      tuplesPMSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
      bufferServerBytesPMSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
      queueSizeMA = new MovingAverageLong(10);
    }
  }

  private final int operatorId;
  public final StatsRevisions statsRevs = new StatsRevisions();
  public OperatorHeartbeat lastHeartbeat;
  public final VersionedLong totalTuplesProcessed = statsRevs.newVersionedLong();
  public final VersionedLong totalTuplesEmitted = statsRevs.newVersionedLong();
  public final VersionedLong currentWindowId = statsRevs.newVersionedLong();
  public final VersionedLong tuplesProcessedPSMA = statsRevs.newVersionedLong();
  public final VersionedLong tuplesEmittedPSMA = statsRevs.newVersionedLong();
  public String recordingId;
  public Stats.CheckpointStats checkpointStats;
  public final MovingAverageLong checkpointTimeMA;
  public final TimedMovingAverageLong cpuNanosPMSMA;
  public final MovingAverageLong latencyMA;
  public final Map<String, PortStatus> inputPortStatusList = new ConcurrentHashMap<>();
  public final Map<String, PortStatus> outputPortStatusList = new ConcurrentHashMap<>();
  public List<OperatorStats> lastWindowedStats = Collections.emptyList();
  public final ConcurrentLinkedQueue<List<OperatorStats>> listenerStats = new ConcurrentLinkedQueue<>();
  public volatile long lastWindowIdChangeTms = 0;
  public final int windowProcessingTimeoutMillis;
  public final ConcurrentLinkedQueue<StatsListener.OperatorResponse> responses = new ConcurrentLinkedQueue<>();
  public List<StatsListener.OperatorResponse> operatorResponses;

  private final LogicalPlan.OperatorMeta operatorMeta;
  private final int throughputCalculationInterval;
  private final int throughputCalculationMaxSamples;


  public OperatorStatus(int operatorId, LogicalPlan.OperatorMeta om)
  {
    this.operatorId = operatorId;
    this.operatorMeta = om;
    LogicalPlan dag = om.getDAG();
    throughputCalculationInterval = dag.getValue(LogicalPlan.THROUGHPUT_CALCULATION_INTERVAL);
    throughputCalculationMaxSamples = dag.getValue(LogicalPlan.THROUGHPUT_CALCULATION_MAX_SAMPLES);
    int heartbeatInterval = dag.getValue(LogicalPlan.HEARTBEAT_INTERVAL_MILLIS);

    cpuNanosPMSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
    latencyMA = new MovingAverageLong(throughputCalculationInterval / heartbeatInterval);
    checkpointTimeMA = new MovingAverageLong(throughputCalculationInterval / heartbeatInterval);
    this.windowProcessingTimeoutMillis = dag.getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS)
      * om.getValue(OperatorContext.TIMEOUT_WINDOW_COUNT);
  }

  public boolean isIdle()
  {
    if (lastHeartbeat != null && DeployState.SHUTDOWN == lastHeartbeat.getState()) {
      return true;
    }
    return false;
  }

  @Override
  public int getOperatorId()
  {
    return operatorId;
  }

  @Override
  public List<OperatorStats> getLastWindowedStats()
  {
    return lastWindowedStats;
  }

  @Override
  public long getCurrentWindowId()
  {
    return currentWindowId.get();
  }

  @Override
  public long getTuplesProcessedPSMA()
  {
    return tuplesProcessedPSMA.get();
  }

  @Override
  public long getTuplesEmittedPSMA()
  {
    return tuplesEmittedPSMA.get();
  }

  @Override
  public double getCpuPercentageMA()
  {
    return this.cpuNanosPMSMA.getAvg();
  }

  @Override
  public long getLatencyMA()
  {
    return this.latencyMA.getAvg();
  }

  @Override
  public List<StatsListener.OperatorResponse> getOperatorResponse()
  {
    return operatorResponses;
  }

  private static class SerializationProxy implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312231635L;
    final int operatorId;
    final LogicalPlan.OperatorMeta operatorMeta;

    private SerializationProxy(OperatorStatus s)
    {
      this.operatorId = s.operatorId;
      this.operatorMeta = s.operatorMeta;
    }

    private Object readResolve() throws java.io.ObjectStreamException
    {
      OperatorStatus s = new OperatorStatus(operatorId, operatorMeta);
      return s;
    }
  }

  private Object writeReplace() throws java.io.ObjectStreamException
  {
    return new SerializationProxy(this);
  }
}
