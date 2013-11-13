/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.plan;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.datatorrent.api.HeartbeatListener;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.stram.EventRecorder.Event;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan.PlanContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestPlanContext implements PlanContext, StorageAgent {
  public List<Runnable> events = new ArrayList<Runnable>();
  public Collection<PTOperator> undeploy;
  public Collection<PTOperator> deploy;
  public Set<PTContainer> releaseContainers;
  public List<Integer> checkpointDeletes = Lists.newArrayList();
  public int backupRequests;

  @Override
  public StorageAgent getStorageAgent() {
    return this;
  }

  @Override
  public void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy) {
    this.undeploy = Sets.newHashSet(undeploy);
    this.deploy = Sets.newHashSet(deploy);
    this.releaseContainers = releaseContainers;
  }

  @Override
  public void dispatch(Runnable r) {
    events.add(r);
  }

  @Override
  public OutputStream getSaveStream(int operatorId, long windowId) throws IOException
  {
    return new OutputStream()
    {
      @Override
      public void write(int b) throws IOException
      {
      }

      @Override
      public void close() throws IOException
      {
        super.close();
        backupRequests++;
      }

    };
  }

  @Override
  public InputStream getLoadStream(int operatorId, long windowId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(int operatorId, long windowId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getMostRecentWindowId(int operatorId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void recordEventAsync(Event ev)
  {
  }

  public static class MockOperatorStatus implements HeartbeatListener.BatchedOperatorStats
  {
    final PTOperator oper;

    public MockOperatorStatus(PTOperator oper)
    {
      this.oper = oper;
    }

    @Override
    public List<OperatorStats> getLastWindowedStats()
    {
      return Collections.emptyList();
    }

    @Override
    public int getOperatorId()
    {
      return oper.getId();
    }

    @Override
    public long getCurrentWindowId()
    {
      return 0;
    }

    public long tuplesProcessedPSMA = 0;

    @Override
    public long getTuplesProcessedPSMA()
    {
      return tuplesProcessedPSMA;
    }

    public long tuplesEmittedPSMA;

    @Override
    public long getTuplesEmittedPSMA()
    {
      return this.tuplesEmittedPSMA;
    }

    @Override
    public double getCpuPercentageMA()
    {
      return 0;
    }

    @Override
    public long getLatencyMA()
    {
      return 0;
    }
  }

}