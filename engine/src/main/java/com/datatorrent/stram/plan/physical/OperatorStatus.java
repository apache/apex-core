/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.StatsRevisions.VersionedLong;
import com.datatorrent.stram.util.MovingAverage.MovingAverageDouble;
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

  public class PortStatus implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312231635L;
    public String portName;
    public long totalTuples;
    public long recordingStartTime = Stats.INVALID_TIME_MILLIS;
    public final TimedMovingAverageLong tuplesPSMA;
    public final TimedMovingAverageLong bufferServerBytesPSMA;

    public PortStatus() {
      tuplesPSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
      bufferServerBytesPSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
    }
  }

  private final int operatorId;
  public final StatsRevisions statsRevs = new StatsRevisions();
  public OperatorHeartbeat lastHeartbeat;
  public final VersionedLong totalTuplesProcessed = statsRevs.new VersionedLong();
  public final VersionedLong totalTuplesEmitted = statsRevs.new VersionedLong();
  public final VersionedLong currentWindowId = statsRevs.new VersionedLong();
  public final VersionedLong tuplesProcessedPSMA = statsRevs.new VersionedLong();
  public final VersionedLong tuplesEmittedPSMA = statsRevs.new VersionedLong();
  public long recordingStartTime = Stats.INVALID_TIME_MILLIS;
  public final MovingAverageDouble cpuPercentageMA;
  public final MovingAverageLong latencyMA;
  public Map<String, PortStatus> inputPortStatusList = new HashMap<String, PortStatus>();
  public Map<String, PortStatus> outputPortStatusList = new HashMap<String, PortStatus>();
  public List<OperatorStats> lastWindowedStats = Collections.emptyList();
  public ConcurrentLinkedQueue<List<OperatorStats>> listenerStats = new ConcurrentLinkedQueue<List<OperatorStats>>();

  private final int throughputCalculationInterval;
  private final int throughputCalculationMaxSamples;
  public int loadIndicator = 0;

  public OperatorStatus(int operatorId, LogicalPlan dag)
  {
    this.operatorId = operatorId;
    throughputCalculationInterval = dag.getValue(LogicalPlan.THROUGHPUT_CALCULATION_INTERVAL);
    throughputCalculationMaxSamples = dag.getValue(LogicalPlan.THROUGHPUT_CALCULATION_MAX_SAMPLES);
    int heartbeatInterval = dag.getValue(LogicalPlan.HEARTBEAT_INTERVAL_MILLIS);

    cpuPercentageMA = new MovingAverageDouble(throughputCalculationInterval / heartbeatInterval);
    latencyMA = new MovingAverageLong(throughputCalculationInterval / heartbeatInterval);
  }

  public boolean isIdle()
  {
    if ((lastHeartbeat != null && DeployState.IDLE.name().equals(lastHeartbeat.getState()))) {
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
    return this.cpuPercentageMA.getAvg();
  }

  @Override
  public long getLatencyMA()
  {
    return this.latencyMA.getAvg();
  }

}
