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
package com.datatorrent.stram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.stram.MockContainer.MockOperatorStats;
import com.datatorrent.stram.StreamingContainerManager.UpdateCheckpointsContext;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

/**
 *
 */
public class CheckpointTest
{
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);

  @Rule
  public TestMeta testMeta = new TestMeta();

  private static class MockInputOperator extends BaseOperator implements InputOperator, Operator.CheckpointNotificationListener
  {
    @OutputPortFieldAnnotation( optional = true)
    public final transient DefaultOutputPort<Object> outport = new DefaultOutputPort<>();
    private transient int windowCount;

    private int checkpointState;

    @Override
    public void beginWindow(long windowId)
    {
      if (++windowCount == 3) {
        BaseOperator.shutdown();
      }
    }

    @Override
    public void emitTuples()
    {
    }

    @Override
    public void beforeCheckpoint(long windowId)
    {
      ++checkpointState;
    }

    @Override
    public void checkpointed(long windowId)
    {
    }

    @Override
    public void committed(long windowId)
    {
    }
  }

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  /**
   * Test saving of operator state at window boundary.
   * @throws Exception
   */
  @Test
  public void testBackup() throws Exception
  {
    AsyncFSStorageAgent storageAgent = new AsyncFSStorageAgent(testMeta.getPath(), null);
    storageAgent.setSyncCheckpoint(true);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, storageAgent);
    dag.setAttribute(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 1);
    dag.setAttribute(LogicalPlan.HEARTBEAT_INTERVAL_MILLIS, 50);
    dag.setAttribute(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    dag.setAttribute(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 50);

    MockInputOperator o1 = dag.addOperator("o1", new MockInputOperator());

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.STATELESS, true);

    dag.addStream("o1.outport", o1.outport, o2.inport1).setLocality(Locality.CONTAINER_LOCAL);

    StramLocalCluster sc = new StramLocalCluster(dag);
    sc.setHeartbeatMonitoringEnabled(false);
    sc.run();

    StreamingContainerManager dnm = sc.dnmgr;
    PhysicalPlan plan = dnm.getPhysicalPlan();
    Assert.assertEquals("number required containers", 1, dnm.getPhysicalPlan().getContainers().size());

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    Set<Long> checkpoints = Sets.newHashSet();
    for (long windowId : storageAgent.getWindowIds(o1p1.getId())) {
      checkpoints.add(windowId);
    }
    Assert.assertEquals("number checkpoints " + checkpoints, 3, checkpoints.size());
    Assert.assertTrue("contains " + checkpoints + " " + Stateless.WINDOW_ID, checkpoints.contains(Stateless.WINDOW_ID));

    PTOperator o2p1 = plan.getOperators(dag.getMeta(o2)).get(0);
    checkpoints = Sets.newHashSet();
    for (long windowId : storageAgent.getWindowIds(o2p1.getId())) {
      checkpoints.add(windowId);
    }
    Assert.assertEquals("number checkpoints " + checkpoints, 1, checkpoints.size());
    Assert.assertEquals("checkpoints " + o2p1, Sets.newHashSet(Stateless.WINDOW_ID), checkpoints);

    Assert.assertEquals("checkpoints " + o1p1 + " " + o1p1.checkpoints, 2, o1p1.checkpoints.size());
    Assert.assertNotNull("checkpoint not null for statefull operator " + o1p1, o1p1.stats.checkpointStats);

    for (Checkpoint cp : o1p1.checkpoints) {
      Object load = storageAgent.load(o1p1.getId(), cp.windowId);
      Assert.assertEquals("Stored Operator and Saved State", load.getClass(), o1p1.getOperatorMeta().getOperator().getClass());
    }
  }

  @Stateless
  public static class StatelessOperator extends GenericTestOperator
  {
  }

  @Test
  public void testUpdateRecoveryCheckpoint() throws Exception
  {
    Clock clock = new SystemClock();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3SL = dag.addOperator("o3SL", StatelessOperator.class);

    dag.addStream("o1.output1", o1.outport1, o2.inport1);
    dag.addStream("o2.output1", o2.outport1, o3SL.inport1);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    for (PTOperator oper : plan.getAllOperators().values()) {
      Assert.assertEquals("activation windowId " + oper, Checkpoint.INITIAL_CHECKPOINT, oper.getRecoveryCheckpoint());
      Assert.assertEquals("checkpoints " + oper, Collections.emptyList(), oper.checkpoints);
    }

    List<PTOperator> nodes1 = plan.getOperators(dag.getMeta(o1));
    Assert.assertNotNull(nodes1);
    Assert.assertEquals(1, nodes1.size());
    PTOperator o1p1 = nodes1.get(0);

    PTOperator o2p1 = plan.getOperators(dag.getMeta(o2)).get(0);
    PTOperator o3SLp1 = plan.getOperators(dag.getMeta(o3SL)).get(0);

    // recovery checkpoint won't update in deploy state
    for (PTOperator oper : plan.getAllOperators().values()) {
      Assert.assertEquals("", PTOperator.State.PENDING_DEPLOY, oper.getState());
    }

    dnm.updateRecoveryCheckpoints(o2p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("no checkpoints " + o2p1, Checkpoint.INITIAL_CHECKPOINT, o2p1.getRecoveryCheckpoint());

    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("no checkpoints " + o1p1, Checkpoint.INITIAL_CHECKPOINT, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("number dependencies " + ctx.visited, 3, ctx.visited.size());

    // adding checkpoints to upstream only does not move recovery checkpoint
    Checkpoint cp3 = new Checkpoint(3L, 0, 0);
    Checkpoint cp5 = new Checkpoint(5L, 0, 0);
    Checkpoint cp4 = new Checkpoint(4L, 0, 0);

    o1p1.checkpoints.add(cp3);
    o1p1.checkpoints.add(cp5);
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, Checkpoint.INITIAL_CHECKPOINT, o1p1.getRecoveryCheckpoint());

    o2p1.checkpoints.add(new Checkpoint(3L, 0, 0));
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, Checkpoint.INITIAL_CHECKPOINT, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + o2p1, Checkpoint.INITIAL_CHECKPOINT, o2p1.getRecoveryCheckpoint());

    // set leaf operator checkpoint
    dnm.addCheckpoint(o3SLp1, cp5);
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, Checkpoint.INITIAL_CHECKPOINT, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + o2p1, Checkpoint.INITIAL_CHECKPOINT, o2p1.getRecoveryCheckpoint());

    // set all operators as active to enable recovery window id update
    for (PTOperator oper : plan.getAllOperators().values()) {
      oper.setState(PTOperator.State.ACTIVE);
    }
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, cp3, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + o2p1, cp3, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + o3SLp1, cp5, o3SLp1.getRecoveryCheckpoint());
    Assert.assertNull("checkpoint null for stateless operator " + o3SLp1, o3SLp1.stats.checkpointStats);

    o2p1.checkpoints.add(cp4);
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, cp3, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + o2p1, cp4, o2p1.getRecoveryCheckpoint());

    o1p1.checkpoints.add(1, cp4);
    Assert.assertEquals(o1p1.checkpoints, getCheckpoints(3L, 4L, 5L));
    dnm.updateRecoveryCheckpoints(o1p1, new UpdateCheckpointsContext(clock), false);
    Assert.assertEquals("checkpoint " + o1p1, cp4, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals(o1p1.checkpoints, getCheckpoints(4L, 5L));

    // out of sequence windowIds should be sorted
    dnm.addCheckpoint(o2p1, new Checkpoint(2L, 0, 0));
    Assert.assertEquals("add first", getCheckpoints(2L, 4L), o2p1.checkpoints);

    dnm.addCheckpoint(o2p1, new Checkpoint(3L, 0, 0));
    Assert.assertEquals("add middle", getCheckpoints(2L, 3L, 4L), o2p1.checkpoints);

    dnm.addCheckpoint(o2p1, new Checkpoint(4L, 0, 0));
    Assert.assertEquals("ignore duplicate", getCheckpoints(2L, 3L, 4L), o2p1.checkpoints);

    dnm.addCheckpoint(o2p1, new Checkpoint(5L, 0, 0));
    Assert.assertEquals("add latest", getCheckpoints(2L, 3L, 4L, 5L), o2p1.checkpoints);

  }

  @Test
  public void testUpdateRecoveryCheckpointWithCycle() throws Exception
  {
    Clock clock = new SystemClock();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    // Simulate a DAG with a loop which has a unifier operator
    TestGeneratorInputOperator o1 = dag.addOperator("o1", TestGeneratorInputOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    GenericTestOperator o3 = dag.addOperator("o3", GenericTestOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);
    DefaultDelayOperator d = dag.addOperator("d", DefaultDelayOperator.class);

    dag.addStream("o1.output1", o1.outport, o2.inport1);
    dag.addStream("o2.output1", o2.outport1, o3.inport1);
    dag.addStream("o3.output1", o3.outport1, o4.inport1);
    dag.addStream("o4.output1", o4.outport1, d.input);
    dag.addStream("d.output", d.output, o2.inport2);
    dag.setOperatorAttribute(o3, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(2));

    dag.validate();
    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    for (PTOperator oper : plan.getAllOperators().values()) {
      Assert.assertEquals("Initial activation windowId" + oper, Checkpoint.INITIAL_CHECKPOINT, oper.getRecoveryCheckpoint());
      Assert.assertEquals("Checkpoints empty" + oper, Collections.emptyList(), oper.checkpoints);
    }

    Checkpoint cp1 = new Checkpoint(1L, 0, 0);
    Checkpoint cp2 = new Checkpoint(2L, 0, 0);

    Map<OperatorMeta, Set<OperatorMeta>> checkpointGroups = dnm.getCheckpointGroups();

    Map<Integer, PTOperator> allOperators = plan.getAllOperators();
    for (PTOperator operator: allOperators.values()) {
      operator.setState(PTOperator.State.ACTIVE);
      operator.checkpoints.add(cp1);
      dnm.updateRecoveryCheckpoints(operator,
          new UpdateCheckpointsContext(clock, false, checkpointGroups), false);
    }

    List<PTOperator> physicalO1 = plan.getOperators(dag.getOperatorMeta("o1"));
    physicalO1.get(0).checkpoints.add(cp2);
    dnm.updateRecoveryCheckpoints(physicalO1.get(0),
        new UpdateCheckpointsContext(clock, false, checkpointGroups), false);

    Assert.assertEquals("Recovery checkpoint updated ", physicalO1.get(0).getRecoveryCheckpoint(), cp1);
  }

  @Test
  public void testUpdateCheckpointsRecovery()
  {
    MockClock clock = new MockClock();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    dag.setAttribute(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 1);

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    StatelessOperator o2SL = dag.addOperator("o2SL", StatelessOperator.class);
    StatelessOperator o3SL = dag.addOperator("o3SL", StatelessOperator.class);
    GenericTestOperator o4 = dag.addOperator("o4", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2SL.inport1);
    dag.addStream("o2SL.outport1", o2SL.outport1, o3SL.inport1, o4.inport1);

    StreamingContainerManager dnm = new StreamingContainerManager(dag, clock);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    for (PTOperator oper : plan.getAllOperators().values()) {
      Assert.assertEquals("activation windowId " + oper, Checkpoint.INITIAL_CHECKPOINT, oper.getRecoveryCheckpoint());
      Assert.assertEquals("checkpoints " + oper, Collections.emptyList(), oper.checkpoints);
    }

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    PTOperator o2SLp1 = plan.getOperators(dag.getMeta(o2SL)).get(0);
    PTOperator o3SLp1 = plan.getOperators(dag.getMeta(o3SL)).get(0);
    PTOperator o4p1 = plan.getOperators(dag.getMeta(o4)).get(0);

    Checkpoint leafCheckpoint = new Checkpoint(2L, 0, 0);
    clock.time = 3;
    o4p1.checkpoints.add(leafCheckpoint);

    UpdateCheckpointsContext ctx;
    dnm.updateRecoveryCheckpoints(o1p1, ctx = new UpdateCheckpointsContext(clock, true, Collections.<OperatorMeta, Set<OperatorMeta>>emptyMap()), false);
    Assert.assertEquals("initial checkpoint " + o1p1, Checkpoint.INITIAL_CHECKPOINT, o1p1.getRecoveryCheckpoint());
    Assert.assertEquals("initial checkpoint " + o2SLp1, leafCheckpoint, o2SLp1.getRecoveryCheckpoint());
    Assert.assertEquals("initial checkpoint " + o3SLp1, new Checkpoint(clock.getTime(), 0, 0), o3SLp1.getRecoveryCheckpoint());
    Assert.assertEquals("number dependencies " + ctx.visited, plan.getAllOperators().size(), ctx.visited.size());

  }


  public List<Checkpoint> getCheckpoints(Long... windowIds)
  {
    List<Checkpoint> list = new ArrayList<>(windowIds.length);
    for (Long windowId : windowIds) {
      list.add(new Checkpoint(windowId, 0, 0));
    }

    return list;
  }

  public class MockClock implements Clock
  {
    public long time = 0;

    @Override
    public long getTime()
    {
      return time;
    }
  }

  @Test
  public void testUpdateCheckpointsProcessingTimeout()
  {
    MockClock clock = new MockClock();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    // set all operators as active to enable recovery window id update
    for (PTOperator oper : plan.getAllOperators().values()) {
      oper.setState(PTOperator.State.ACTIVE);
    }

    List<PTOperator> partitions = plan.getOperators(dag.getMeta(o1));
    Assert.assertNotNull(partitions);
    Assert.assertEquals(1, partitions.size());
    PTOperator o1p1 = partitions.get(0);

    partitions = plan.getOperators(dag.getMeta(o2));
    Assert.assertNotNull(partitions);
    Assert.assertEquals(1, partitions.size());
    PTOperator o2p1 = partitions.get(0);

    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertTrue("no blocked operators", ctx.blocked.isEmpty());

    o1p1.stats.statsRevs.checkout();
    o1p1.stats.currentWindowId.set(1);
    o1p1.stats.lastWindowIdChangeTms = 1;
    o1p1.stats.statsRevs.commit();

    clock.time = o1p1.stats.windowProcessingTimeoutMillis + 1;

    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("o2 blocked", Sets.newHashSet(o2p1), ctx.blocked);

    // assign future activation window (state-less or at-most-once).
    Checkpoint cp2 = o2p1.getRecoveryCheckpoint();
    o2p1.setRecoveryCheckpoint(new Checkpoint(o1p1.getRecoveryCheckpoint().windowId + 1, cp2.applicationWindowCount, cp2.checkpointWindowCount));
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("no operators blocked (o2 activation window ahead)", Sets.newHashSet(), ctx.blocked);

    // reset to blocked
    o2p1.setRecoveryCheckpoint(cp2);
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("o2 blocked", Sets.newHashSet(o2p1), ctx.blocked);

    clock.time++;
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("operators blocked", Sets.newHashSet(o1p1, o2p1), ctx.blocked);

    o2p1.stats.statsRevs.checkout();
    o2p1.stats.currentWindowId.set(o1p1.stats.getCurrentWindowId());
    o2p1.stats.statsRevs.commit();

    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("operators blocked", Sets.newHashSet(o1p1), ctx.blocked);

    clock.time--;
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx, false);
    Assert.assertEquals("operators blocked", Sets.newHashSet(), ctx.blocked);
  }

  @Test
  public void testBlockedOperatorContainerRestart()
  {
    MockClock clock = new MockClock();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.setOperatorAttribute(o1, OperatorContext.TIMEOUT_WINDOW_COUNT, 2);

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);

    dag.addStream("o1.outport1", o1.outport1, o2.inport1);

    StreamingContainerManager scm = new StreamingContainerManager(dag, false, clock);
    PhysicalPlan plan = scm.getPhysicalPlan();

    List<PTContainer> containers = plan.getContainers();
    Assert.assertEquals("Number of containers", 2, containers.size());

    Map<PTContainer, MockContainer> mockContainers = Maps.newHashMap();
    // allocate/assign all containers
    for (PTContainer c : containers) {
      MockContainer mc = new MockContainer(scm, c);
      mockContainers.put(c, mc);
    }
    // deploy all containers
    for (MockContainer mc : mockContainers.values()) {
      mc.deploy();
    }

    PTOperator o1p1 = plan.getOperators(dag.getMeta(o1)).get(0);
    MockContainer mc1 = mockContainers.get(o1p1.getContainer());
    MockOperatorStats o1p1mos = mockContainers.get(o1p1.getContainer()).stats(o1p1.getId());
    o1p1mos.currentWindowId(1).deployState(DeployState.ACTIVE);
    clock.time = 10;
    mc1.sendHeartbeat();

    Assert.assertEquals(clock.time, o1p1.stats.lastWindowIdChangeTms);
    Assert.assertEquals(1, o1p1.stats.currentWindowId.get());
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());
    int timeoutMs = dag.getMeta(o1).getValue(OperatorContext.TIMEOUT_WINDOW_COUNT) * dag.getValue(DAG.STREAMING_WINDOW_SIZE_MILLIS);
    Assert.assertEquals("processing timeout", timeoutMs, o1p1.stats.windowProcessingTimeoutMillis);

    clock.time += timeoutMs;
    mc1.sendHeartbeat();
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());
    Assert.assertEquals(10, o1p1.stats.lastWindowIdChangeTms);
    scm.monitorHeartbeat(false);
    Assert.assertTrue(scm.containerStopRequests.isEmpty());

    clock.time++;
    mc1.sendHeartbeat();
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());
    scm.monitorHeartbeat(false);
    Assert.assertTrue(scm.containerStopRequests.containsKey(o1p1.getContainer().getExternalId()));

  }

  @Test
  public void testBeforeCheckpointNotification() throws IOException, ClassNotFoundException
  {
    FSStorageAgent storageAgent = new FSStorageAgent(testMeta.getPath(), null);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, storageAgent);
    dag.setAttribute(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 1);
    dag.setAttribute(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 50);

    MockInputOperator o1 = dag.addOperator("o1", new MockInputOperator());

    GenericTestOperator o2 = dag.addOperator("o2", GenericTestOperator.class);
    dag.setOperatorAttribute(o2, OperatorContext.STATELESS, true);

    dag.addStream("o1.outport", o1.outport, o2.inport1);

    StramLocalCluster sc = new StramLocalCluster(dag);
    sc.setHeartbeatMonitoringEnabled(false);
    sc.run();

    StreamingContainerManager dnm = sc.dnmgr;
    PhysicalPlan plan = dnm.getPhysicalPlan();
    List<PTOperator> o1ps = plan.getOperators(dag.getMeta(o1));
    Assert.assertEquals("Number partitions", 1, o1ps.size());

    PTOperator o1p1 = o1ps.get(0);
    long[] ckWIds = storageAgent.getWindowIds(o1p1.getId());
    Arrays.sort(ckWIds);
    int expectedState = 0;
    for (long windowId : ckWIds) {
      Object ckState = storageAgent.load(o1p1.getId(), windowId);
      Assert.assertEquals("Checkpointed state class", MockInputOperator.class, ckState.getClass());
      Assert.assertEquals("Checkpoint state", expectedState++, ((MockInputOperator)ckState).checkpointState);
    }
  }

}
