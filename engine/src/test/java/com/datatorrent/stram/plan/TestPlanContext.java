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
package com.datatorrent.stram.plan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.Journal.Recoverable;
import com.datatorrent.stram.api.StramEvent;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan.PlanContext;

public class TestPlanContext implements PlanContext, StorageAgent, Serializable
{
  public List<Runnable> events = new ArrayList<>();
  public Collection<PTOperator> undeploy;
  public Collection<PTOperator> deploy;
  public Set<PTContainer> releaseContainers;
  public List<Integer> checkpointDeletes = Lists.newArrayList();
  public Map<Integer, Map<Long, byte[]>> checkpoints = Maps.newHashMap();
  public int backupRequests;

  @Override
  public void deploy(Set<PTContainer> releaseContainers, Collection<PTOperator> undeploy, Set<PTContainer> startContainers, Collection<PTOperator> deploy)
  {
    this.undeploy = Sets.newHashSet(undeploy);
    this.deploy = Sets.newHashSet(deploy);
    this.releaseContainers = releaseContainers;
  }

  @Override
  public void dispatch(Runnable r)
  {
    events.add(r);
  }

  @Override
  public void save(Object operator, final int operatorId, final long windowId) throws IOException
  {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try {
      FSStorageAgent.store(byteArrayOutputStream, operator);
    } finally {
      byteArrayOutputStream.close();
    }
    backupRequests++;
    Map<Long, byte[]> m = checkpoints.get(operatorId);
    if (m == null) {
      m = Maps.newHashMap();
      checkpoints.put(operatorId, m);
    }
    m.put(windowId, byteArrayOutputStream.toByteArray());
  }

  @Override
  public Object load(int operatorId, long windowId) throws IOException
  {
    Map<Long, byte[]> m = checkpoints.get(operatorId);
    if (m == null || !m.containsKey(windowId)) {
      throw new IOException("checkpoint not found operatorId=" + operatorId + " windowId=" + windowId);
    }
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(m.get(windowId));
    try {
      return FSStorageAgent.retrieve(byteArrayInputStream);
    } finally {
      byteArrayInputStream.close();
    }
  }

  @Override
  public void delete(int operatorId, long windowId) throws IOException
  {
    checkpointDeletes.add(operatorId);
  }

  @Override
  public long[] getWindowIds(int operatorId) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void recordEventAsync(StramEvent ev)
  {
  }

  @Override
  public void writeJournal(Recoverable operation)
  {
  }

  @Override
  public void addOperatorRequest(PTOperator oper, StramToNodeRequest request)
  {
  }

  public static class MockOperatorStatus implements StatsListener.BatchedOperatorStats
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

    @Override
    public List<StatsListener.OperatorResponse> getOperatorResponse()
    {
      return null;
    }
  }

}
