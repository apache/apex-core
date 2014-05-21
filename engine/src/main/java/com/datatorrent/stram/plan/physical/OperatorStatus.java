/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan.physical;

import com.datatorrent.api.Context.Counters;
import com.datatorrent.api.Stats;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.StatsRevisions.VersionedLong;
import com.datatorrent.stram.util.MovingAverage.MovingAverageLong;
import com.datatorrent.stram.util.MovingAverage.TimedMovingAverageLong;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    public long recordingStartTime = Stats.INVALID_TIME_MILLIS;
    public final TimedMovingAverageLong tuplesPMSMA;
    public final TimedMovingAverageLong bufferServerBytesPMSMA;

    public PortStatus() {
      tuplesPMSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
      bufferServerBytesPMSMA = new TimedMovingAverageLong(throughputCalculationMaxSamples, throughputCalculationInterval);
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
  public long recordingStartTime = Stats.INVALID_TIME_MILLIS;
  public final TimedMovingAverageLong cpuNanosPMSMA;
  public final MovingAverageLong latencyMA;
  public final Map<String, PortStatus> inputPortStatusList = new ConcurrentHashMap<String, PortStatus>();
  public final Map<String, PortStatus> outputPortStatusList = new ConcurrentHashMap<String, PortStatus>();
  public List<OperatorStats> lastWindowedStats = Collections.emptyList();
  public final ConcurrentLinkedQueue<List<OperatorStats>> listenerStats = new ConcurrentLinkedQueue<List<OperatorStats>>();
  public volatile long lastWindowIdChangeTms = 0;
  public final int windowProcessingTimeoutMillis;

  private final LogicalPlan.OperatorMeta operatorMeta;
  private final int throughputCalculationInterval;
  private final int throughputCalculationMaxSamples;
  public int loadIndicator = 0;
  public Counters counters;

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
    this.windowProcessingTimeoutMillis = dag.getValue(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS) * om.getValue(OperatorContext.TIMEOUT_WINDOW_COUNT);
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

  private static class SerializationProxy implements java.io.Serializable
  {
    private static final long serialVersionUID = 201312231635L;
    final int operatorId;
    final LogicalPlan.OperatorMeta operatorMeta;

    private SerializationProxy(OperatorStatus s) {
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
