/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.stram.MockContainer.MockOperatorStats;
import com.datatorrent.stram.StramLocalCluster.LocalStramChild;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.StreamingContainerManager.UpdateCheckpointsContext;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.ContainerStats;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat;
import com.datatorrent.stram.api.StreamingContainerUmbilicalProtocol.OperatorHeartbeat.DeployState;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.ManualScheduledExecutorService;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

/**
 *
 */
public class CheckpointTest
{
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);
  @Rule public TestMeta testMeta = new TestMeta();

  /**
   *
   * @throws IOException
   */
  @Before
  public void setupEachTest() throws IOException
  {
    try {
      FileContext.getLocalFSFileContext().delete(
              new Path(new File(testMeta.dir).getAbsolutePath()), true);
    }
    catch (Exception e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }
    //StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    //StramChild.eventloop.stop();
  }

  /**
   * Test saving of operator state at window boundary.
   *
   * @throws Exception
   */
  @Test
  public void testBackup() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, testMeta.dir);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 1);
    // node with no inputs will be connected to window generator
    TestGeneratorInputOperator m1 = dag.addOperator("node1", TestGeneratorInputOperator.class);
    m1.setMaxTuples(2);
    StreamingContainerManager dnm = new StreamingContainerManager(dag);

    Assert.assertEquals("number required containers", 1, dnm.getPhysicalPlan().getContainers().size());

    String containerId = "container1";
    StramChildAgent sca = dnm.assignContainer(new ContainerResource(0, containerId, "localhost", 0, null), InetSocketAddress.createUnresolved("localhost", 0));
    Assert.assertNotNull(sca);

    ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    WindowGenerator wingen = StramTestSupport.setupWindowGenerator(mses);
    wingen.setCheckpointCount(1, 0);
    LocalStramChild container = new LocalStramChild(containerId, null, wingen);

    container.setup(sca.getInitContext());
    // push deploy
    List<OperatorDeployInfo> deployInfo = sca.getDeployInfoList(sca.container.getOperators());
    Assert.assertEquals("size " + deployInfo, 1, deployInfo.size());

    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.deployRequest = deployInfo;

    container.processHeartbeatResponse(rsp);

    OperatorHeartbeat ohb = new OperatorHeartbeat();
    ohb.setNodeId(deployInfo.get(0).id);
    ohb.setState(OperatorHeartbeat.DeployState.ACTIVE.name());

    ContainerStats cstats = new ContainerStats(containerId);
    cstats.addNodeStats(ohb);

    ContainerHeartbeat hb = new ContainerHeartbeat();
    hb.setContainerStats(cstats);

    dnm.processHeartbeat(hb); // mark deployed

    mses.tick(1); // begin window 1

    Assert.assertEquals("number operators", 1, container.getNodes().size());
    Operator node = container.getNode(deployInfo.get(0).id);
    OperatorContext context = container.getNodeContext(deployInfo.get(0).id);

    Assert.assertNotNull("deployed " + deployInfo.get(0), node);
    Assert.assertEquals("operator id", deployInfo.get(0).id, context.getId());
    Assert.assertEquals("maxTupes", 2, ((TestGeneratorInputOperator)node).getMaxTuples());

    mses.tick(1); // end window 1, begin window 2
    // await end window 1 to ensure backup is executed at window 2
    StramTestSupport.waitForWindowComplete(context, 1);
    int operatorid = context.getId();
    rsp = new ContainerHeartbeatResponse();

    mses.tick(1); // end window 2 begin window 3
    StramTestSupport.waitForWindowComplete(context, 2);
    Assert.assertEquals("window 2", 2, context.getLastProcessedWindowId());

    ohb.getOperatorStatsContainer().clear();
    context.drainStats(ohb.getOperatorStatsContainer());
    List<com.datatorrent.api.Stats.OperatorStats> stats = ohb.getOperatorStatsContainer();
    Assert.assertEquals("windows stats " + stats, 3, stats.size());
    Assert.assertEquals("windowId " + stats.get(2), 2, stats.get(2).windowId);
    Assert.assertEquals("checkpointedWindowId " + stats.get(2), 1, stats.get(2).checkpoint.getWindowId()); // lags windowId
    dnm.processHeartbeat(hb); // propagate checkpoint

    Thread.sleep(20); // file close delay?
    File cpFile1 = new File(testMeta.dir, LogicalPlan.SUBDIR_CHECKPOINTS + "/" + operatorid + "/" + Codec.getStringWindowId(1));
    Assert.assertTrue("checkpoint file not found: " + cpFile1, cpFile1.exists() && cpFile1.isFile());

    ohb.setState(OperatorHeartbeat.DeployState.ACTIVE.name());

    container.processHeartbeatResponse(rsp);
    mses.tick(1); // end window 3, begin window 4
    StramTestSupport.waitForWindowComplete(context, 3);
    Assert.assertEquals("window 3", 3, context.getLastProcessedWindowId());

    Thread.sleep(20); // file close delay?
    File cpFile2 = new File(testMeta.dir, LogicalPlan.SUBDIR_CHECKPOINTS + "/" + operatorid + "/" + Codec.getStringWindowId(2));
    Assert.assertTrue("checkpoint file not found: " + cpFile2, cpFile2.exists() && cpFile2.isFile());

    ohb.getOperatorStatsContainer().clear();
    context.drainStats(ohb.getOperatorStatsContainer());
    stats = ohb.getOperatorStatsContainer();
    Assert.assertEquals("windows stats " + stats, 1, stats.size());
    Assert.assertEquals("windowId " + stats.get(0), 3, stats.get(0).windowId);
    Assert.assertEquals("checkpointedWindowId " + stats.get(0), 2, stats.get(0).checkpoint.getWindowId()); // lags windowId
    dnm.processHeartbeat(hb); // propagate checkpoint

    // purge checkpoints
    dnm.monitorHeartbeat();

    Assert.assertTrue("checkpoint file not purged: " + cpFile1, !cpFile1.exists());
    Assert.assertTrue("checkpoint file purged: " + cpFile2, cpFile2.exists() && cpFile2.isFile());

    LOG.debug("Shutdown container {}", container.getContainerId());
    container.teardown();
  }

  @Test
  public void testUpdateRecoveryCheckpoint() throws Exception
  {
    Clock clock = new SystemClock();
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();

    for (PTOperator oper : plan.getAllOperators().values()) {
      Assert.assertEquals("activation windowId " + oper, Checkpoint.INITIAL_CHECKPOINT, oper.getRecoveryCheckpoint());
      Assert.assertEquals("checkpoints " + oper, Collections.emptyList(), oper.checkpoints);
    }

    List<PTOperator> nodes1 = plan.getOperators(dag.getMeta(node1));
    Assert.assertNotNull(nodes1);
    Assert.assertEquals(1, nodes1.size());
    PTOperator pnode1 = nodes1.get(0);

    List<PTOperator> nodes2 = plan.getOperators(dag.getMeta(node2));
    Assert.assertNotNull(nodes2);
    Assert.assertEquals(1, nodes2.size());
    PTOperator pnode2 = nodes2.get(0);

    // set all operators as active to enable recovery window id update
    for (PTOperator oper : plan.getAllOperators().values()) {
      oper.setState(PTOperator.State.ACTIVE);
    }

    dnm.updateRecoveryCheckpoints(pnode2, new UpdateCheckpointsContext(clock));
    Assert.assertEquals("no checkpoints " + pnode2, Checkpoint.INITIAL_CHECKPOINT, pnode2.getRecoveryCheckpoint());

    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(pnode1, ctx);
    Assert.assertEquals("no checkpoints " + pnode1, Checkpoint.INITIAL_CHECKPOINT, pnode1.getRecoveryCheckpoint());
    Assert.assertEquals("number dependencies " + ctx.visited, 2, ctx.visited.size());

    // adding checkpoints to upstream only does not move recovery checkpoint
    pnode1.checkpoints.add(new Checkpoint(3L, 0, 0));
    pnode1.checkpoints.add(new Checkpoint(5L, 0, 0));
    dnm.updateRecoveryCheckpoints(pnode1, new UpdateCheckpointsContext(clock));
    Assert.assertEquals("no checkpoints " + pnode1, Checkpoint.INITIAL_CHECKPOINT, pnode1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + pnode1, Checkpoint.INITIAL_CHECKPOINT, pnode1.getRecoveryCheckpoint());

    pnode2.checkpoints.add(new Checkpoint(3L, 0, 0));
    dnm.updateRecoveryCheckpoints(pnode1, new UpdateCheckpointsContext(clock));
    Assert.assertEquals("checkpoint pnode1", 3L, pnode1.getRecoveryCheckpoint().windowId);
    Assert.assertEquals("checkpoint " + pnode1, 3L, pnode1.getRecoveryCheckpoint().windowId);

    pnode2.checkpoints.add(new Checkpoint(4L, 0, 0));
    dnm.updateRecoveryCheckpoints(pnode1, new UpdateCheckpointsContext(clock));
    Assert.assertEquals("checkpoint pnode1", 3L, pnode1.getRecoveryCheckpoint().windowId);
    Assert.assertEquals("checkpoint " + pnode1, 3L, pnode1.getRecoveryCheckpoint().windowId);

    pnode1.checkpoints.add(1, new Checkpoint(4L, 0, 0));
    Assert.assertEquals(pnode1.checkpoints, getCheckpoints(new Long[] {3L, 4L, 5L}));
    dnm.updateRecoveryCheckpoints(pnode1, new UpdateCheckpointsContext(clock));
    Assert.assertEquals("checkpoint pnode1", 4L, pnode1.getRecoveryCheckpoint().windowId);
    Assert.assertEquals("checkpoint " + pnode1, 4L, pnode1.getRecoveryCheckpoint().windowId);
    Assert.assertEquals(pnode1.checkpoints, getCheckpoints(new Long[] {4L, 5L}));

    // out of sequence windowIds should be sorted
    dnm.addCheckpoint(pnode2, new Checkpoint(2L, 0, 0));
    Assert.assertEquals("add first", getCheckpoints(new Long[] {2L, 4L}), pnode2.checkpoints);

    dnm.addCheckpoint(pnode2, new Checkpoint(3L, 0, 0));
    Assert.assertEquals("add middle", getCheckpoints(new Long[] {2L, 3L, 4L}), pnode2.checkpoints);

    dnm.addCheckpoint(pnode2, new Checkpoint(4L, 0, 0));
    Assert.assertEquals("ignore duplicate", getCheckpoints(new Long[] {2L, 3L, 4L}), pnode2.checkpoints);

    dnm.addCheckpoint(pnode2, new Checkpoint(5L, 0, 0));
    Assert.assertEquals("add latest", getCheckpoints(new Long[] {2L, 3L, 4L, 5L}), pnode2.checkpoints);

  }

  public List<Checkpoint> getCheckpoints(Long[] windowIds)
  {
    List<Checkpoint> list = new ArrayList<Checkpoint>(windowIds.length);
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
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

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
    dnm.updateRecoveryCheckpoints(o1p1, ctx);
    Assert.assertTrue("no blocked operators", ctx.blocked.isEmpty());

    o1p1.stats.statsRevs.checkout();
    o1p1.stats.currentWindowId.set(1);
    o1p1.stats.lastWindowIdChangeTms = 1;
    o1p1.stats.statsRevs.commit();

    clock.time = o1p1.stats.windowProcessingTimeoutMillis + 1;

    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx);
    Assert.assertEquals("operators blocked", Sets.newHashSet(o2p1), ctx.blocked);

    clock.time++;
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx);
    Assert.assertEquals("operators blocked", Sets.newHashSet(o1p1, o2p1), ctx.blocked);

    o2p1.stats.statsRevs.checkout();
    o2p1.stats.currentWindowId.set(o1p1.stats.getCurrentWindowId());
    o2p1.stats.statsRevs.commit();

    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx);
    Assert.assertEquals("operators blocked", Sets.newHashSet(o1p1), ctx.blocked);

    clock.time--;
    ctx = new UpdateCheckpointsContext(clock);
    dnm.updateRecoveryCheckpoints(o1p1, ctx);
    Assert.assertEquals("operators blocked", Sets.newHashSet(), ctx.blocked);

  }

  @Test
  public void testBlockedOperatorContainerRestart()
  {
    MockClock clock = new MockClock();
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(LogicalPlan.APPLICATION_PATH, testMeta.dir);

    GenericTestOperator o1 = dag.addOperator("o1", GenericTestOperator.class);
    dag.setAttribute(o1, OperatorContext.TIMEOUT_WINDOW_COUNT, 2);

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
    scm.monitorHeartbeat();
    Assert.assertTrue(scm.containerStopRequests.isEmpty());

    clock.time++;
    mc1.sendHeartbeat();
    Assert.assertEquals(PTOperator.State.ACTIVE, o1p1.getState());
    scm.monitorHeartbeat();
    Assert.assertTrue(scm.containerStopRequests.containsKey(o1p1.getContainer().getExternalId()));

  }


}
