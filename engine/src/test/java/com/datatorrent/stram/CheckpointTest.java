/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.OperatorStats;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.engine.WindowGenerator;
import com.datatorrent.stram.OperatorDeployInfo;
import com.datatorrent.stram.PhysicalPlan;
import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StramChildAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.PhysicalPlan.PTOperator;
import com.datatorrent.stram.StramLocalCluster.LocalStramChild;
import com.datatorrent.stram.StreamingContainerManager.ContainerResource;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeat;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.datatorrent.stram.StreamingContainerUmbilicalProtocol.StreamingNodeHeartbeat;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.ManualScheduledExecutorService;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.api.Operator;

/**
 *
 */
public class CheckpointTest
{
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);
  private static File testWorkDir = new File("target", CheckpointTest.class.getName());

  @BeforeClass
  public static void setup()
  {
    try {
      FileContext.getLocalFSFileContext().delete(
              new Path(testWorkDir.getAbsolutePath()), true);
    }
    catch (Exception e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }
  }

  /**
   *
   * @throws IOException
   */
  @Before
  public void setupEachTest() throws IOException
  {
    StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    StramChild.eventloop.stop();
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
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(1);
    // node with no inputs will be connected to window generator
    TestGeneratorInputOperator m1 = dag.addOperator("node1", TestGeneratorInputOperator.class);
    m1.setMaxTuples(2);
    dag.getAttributes().attr(LogicalPlan.APPLICATION_PATH).set(testWorkDir.getPath());
    StreamingContainerManager dnm = new StreamingContainerManager(dag);

    Assert.assertEquals("number required containers", 1, dnm.getPhysicalPlan().getContainers().size());

    String containerId = "container1";
    StramChildAgent sca = dnm.assignContainer(new ContainerResource(0, containerId, "localhost", 0), InetSocketAddress.createUnresolved("localhost", 0));
    Assert.assertNotNull(sca);

    ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    WindowGenerator wingen = StramTestSupport.setupWindowGenerator(mses);
    wingen.setCheckpointCount(1);
    LocalStramChild container = new LocalStramChild(containerId, null, wingen);

    container.setup(sca.getInitContext());
    // push deploy
    List<OperatorDeployInfo> deployInfo = sca.getDeployInfo();
    Assert.assertEquals("size " + deployInfo, 1, deployInfo.size());

    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.deployRequest = deployInfo;

    container.processHeartbeatResponse(rsp);

    StreamingNodeHeartbeat ohb = new StreamingNodeHeartbeat();
    ohb.setNodeId(deployInfo.get(0).id);
    ohb.setState(StreamingNodeHeartbeat.DNodeState.ACTIVE.name());

    ContainerHeartbeat hb = new ContainerHeartbeat();
    hb.setContainerId(containerId);
    hb.setDnodeEntries(Collections.singletonList(ohb));

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

    ohb.getWindowStats().clear();
    context.drainHeartbeatCounters(ohb.getWindowStats());
    List<OperatorStats> stats = ohb.getWindowStats();
    Assert.assertEquals("windows stats " + stats, 3, stats.size());
    Assert.assertEquals("windowId " + stats.get(2), 2, stats.get(2).windowId);
    Assert.assertEquals("checkpointedWindowId " + stats.get(2), 1, stats.get(2).checkpointedWindowId); // lags windowId
    dnm.processHeartbeat(hb); // propagate checkpoint

    Thread.sleep(20); // file close delay?
    File cpFile1 = new File(testWorkDir, LogicalPlan.SUBDIR_CHECKPOINTS + "/" + operatorid + "/1");
    Assert.assertTrue("checkpoint file not found: " + cpFile1, cpFile1.exists() && cpFile1.isFile());

    ohb.setState(StreamingNodeHeartbeat.DNodeState.ACTIVE.name());

    container.processHeartbeatResponse(rsp);
    mses.tick(1); // end window 3, begin window 4
    StramTestSupport.waitForWindowComplete(context, 3);
    Assert.assertEquals("window 3", 3, context.getLastProcessedWindowId());

    Thread.sleep(20); // file close delay?
    File cpFile2 = new File(testWorkDir, LogicalPlan.SUBDIR_CHECKPOINTS + "/" + operatorid + "/2");
    Assert.assertTrue("checkpoint file not found: " + cpFile2, cpFile2.exists() && cpFile2.isFile());

    ohb.getWindowStats().clear();
    context.drainHeartbeatCounters(ohb.getWindowStats());
    stats = ohb.getWindowStats();
    Assert.assertEquals("windows stats " + stats, 1, stats.size());
    Assert.assertEquals("windowId " + stats.get(0), 3, stats.get(0).windowId);
    Assert.assertEquals("checkpointedWindowId " + stats.get(0), 2, stats.get(0).checkpointedWindowId); // lags windowId
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
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator node1 = dag.addOperator("node1", GenericTestOperator.class);
    GenericTestOperator node2 = dag.addOperator("node2", GenericTestOperator.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1);

    StreamingContainerManager dnm = new StreamingContainerManager(dag);
    PhysicalPlan plan = dnm.getPhysicalPlan();
    List<PTOperator> nodes1 = plan.getOperators(dag.getMeta(node1));
    Assert.assertNotNull(nodes1);
    Assert.assertEquals(1, nodes1.size());
    PTOperator pnode1 = nodes1.get(0);

    List<PTOperator> nodes2 = plan.getOperators(dag.getMeta(node2));
    Assert.assertNotNull(nodes2);
    Assert.assertEquals(1, nodes2.size());
    PTOperator pnode2 = nodes2.get(0);

    long cp = dnm.updateRecoveryCheckpoints(pnode2, new HashSet<PTOperator>(), new MutableLong());
    Assert.assertEquals("no checkpoints " + pnode2, 0, cp);

    HashSet<PTOperator> s = new HashSet<PTOperator>();
    cp = dnm.updateRecoveryCheckpoints(pnode1, s, new MutableLong());
    Assert.assertEquals("no checkpoints " + pnode1, 0, cp);
    Assert.assertEquals("number dependencies " + s, 2, s.size());

    // adding checkpoints to upstream only does not move recovery checkpoint
    pnode1.checkpointWindows.add(3L);
    pnode1.checkpointWindows.add(5L);
    cp = dnm.updateRecoveryCheckpoints(pnode1, new HashSet<PTOperator>(), new MutableLong());
    Assert.assertEquals("no checkpoints " + pnode1, 0L, cp);
    Assert.assertEquals("checkpoint " + pnode1, 0, pnode1.getRecoveryCheckpoint());

    pnode2.checkpointWindows.add(3L);
    cp = dnm.updateRecoveryCheckpoints(pnode1, new HashSet<PTOperator>(), new MutableLong());
    Assert.assertEquals("checkpoint pnode1", 3L, cp);
    Assert.assertEquals("checkpoint " + pnode1, 3L, pnode1.getRecoveryCheckpoint());

    pnode2.checkpointWindows.add(4L);
    cp = dnm.updateRecoveryCheckpoints(pnode1, new HashSet<PTOperator>(), new MutableLong());
    Assert.assertEquals("checkpoint pnode1", 3L, cp);
    Assert.assertEquals("checkpoint " + pnode1, 3L, pnode1.getRecoveryCheckpoint());

    pnode1.checkpointWindows.add(1, 4L);
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[] {3L, 4L, 5L}));
    cp = dnm.updateRecoveryCheckpoints(pnode1, new HashSet<PTOperator>(), new MutableLong());
    Assert.assertEquals("checkpoint pnode1", 4L, cp);
    Assert.assertEquals("checkpoint " + pnode1, 4L, pnode1.getRecoveryCheckpoint());
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[] {4L, 5L}));

    // out of sequence windowIds should be sorted
    dnm.addCheckpoint(pnode2, 2L);
    Assert.assertEquals("add first", Arrays.asList(new Long[] {2L, 4L}), pnode2.checkpointWindows);

    dnm.addCheckpoint(pnode2, 3L);
    Assert.assertEquals("add middle", Arrays.asList(new Long[] {2L, 3L, 4L}), pnode2.checkpointWindows);

    dnm.addCheckpoint(pnode2, 4L);
    Assert.assertEquals("ignore duplicate", Arrays.asList(new Long[] {2L, 3L, 4L}), pnode2.checkpointWindows);

    dnm.addCheckpoint(pnode2, 5L);
    Assert.assertEquals("add latest", Arrays.asList(new Long[] {2L, 3L, 4L, 5L}), pnode2.checkpointWindows);

  }

}
