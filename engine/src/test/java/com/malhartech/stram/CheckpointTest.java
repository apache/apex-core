/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.malhartech.dag.DAG;
import com.malhartech.dag.GenericTestModule;
import com.malhartech.dag.Module;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.TestGeneratorInputModule;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stream.StramTestSupport;

/**
 *
 */
public class CheckpointTest {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);

  private static File testWorkDir = new File("target", CheckpointTest.class.getName());

  @BeforeClass
  public static void setup() {
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(testWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      throw new RuntimeException("could not cleanup test dir", e);
    }
  }

  /**
   * Test saving of node state at window boundary.
   *
   * @throws Exception
   */
  @Test
  public void testBackup() throws Exception
  {
    DAG dag = new DAG();
    // node with no inputs will be connected to window generator
    dag.addOperator("node1", TestGeneratorInputModule.class)
        .setProperty("maxTuples", "1");
    ModuleManager dnm = new ModuleManager(dag);

    Assert.assertEquals("number required containers", 1, dnm.getNumRequiredContainers());

    String containerId = "container1";
    StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", 0));
    ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    WindowGenerator wingen = StramTestSupport.setupWindowGenerator(mses);
    LocalStramChild container = new LocalStramChild(containerId, null, wingen);
    cc.setCheckpointDfsPath(testWorkDir.getPath());
    container.setup(cc);

    mses.tick(1); // begin window 1

    Assert.assertEquals("number nodes", 1, container.getNodes().size());
    Module node = container.getNode(cc.nodeList.get(0).id);
    ModuleContext context = container.getNodeContext(cc.nodeList.get(0).id);

    Assert.assertNotNull("node deployed " + cc.nodeList.get(0), node);
    Assert.assertEquals("nodeId", cc.nodeList.get(0).id, context.getId());
    Assert.assertEquals("maxTupes", 1, ((TestGeneratorInputModule)node).getMaxTuples());

    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(context.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.nodeRequests = Collections.singletonList(backupRequest);
    container.processHeartbeatResponse(rsp);

    mses.tick(1); // end window 1, begin window 2

    // node to move to next window before we verify the checkpoint state
    // if (node.context.getLastProcessedWindowId() < 2) {
    // Thread.sleep(500);
    // }

    Assert.assertTrue("node >= window 1",
        1 <= context.getLastProcessedWindowId());

    File expectedFile = new File(testWorkDir, backupRequest.getNodeId() + "/1");
    Assert.assertTrue("checkpoint file not found: " + expectedFile, expectedFile.exists() && expectedFile.isFile());

    LOG.debug("Shutdown container {}", container.getContainerId());
    container.deactivate();
    container.teardown();
  }

  @Test
  public void testRecoveryCheckpoint() throws Exception
  {
    DAG dag = new DAG();

    Operator node1 = dag.addOperator("node1", GenericTestModule.class);
    Operator node2 = dag.addOperator("node2", GenericTestModule.class);

    dag.addStream("n1n2")
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node2.getInput(GenericTestModule.INPUT1));

    ModuleManager dnm = new ModuleManager(dag);
    PhysicalPlan deployer = dnm.getTopologyDeployer();
    List<PTOperator> nodes1 = deployer.getOperators(node1);
    Assert.assertNotNull(nodes1);
    Assert.assertEquals(1, nodes1.size());
    PTOperator pnode1 = nodes1.get(0);

    List<PTOperator> nodes2 = deployer.getOperators(node2);
    Assert.assertNotNull(nodes2);
    Assert.assertEquals(1, nodes2.size());
    PTOperator pnode2 = nodes2.get(0);

    Map<PTOperator, Long> checkpoints = new HashMap<PTOperator, Long>();
    long cp = dnm.getRecoveryCheckpoint(pnode2, checkpoints);
    Assert.assertEquals("no checkpoints " + pnode2, 0, cp);

    cp = dnm.getRecoveryCheckpoint(pnode1, new HashMap<PTOperator, Long>());
    Assert.assertEquals("no checkpoints " + pnode1, 0, cp);

    // adding checkpoints to upstream only does not move recovery checkpoint
    pnode1.checkpointWindows.add(3L);
    pnode1.checkpointWindows.add(5L);
    cp = dnm.getRecoveryCheckpoint(pnode1, new HashMap<PTOperator, Long>());
    Assert.assertEquals("no checkpoints " + pnode1, 0L, cp);

    pnode2.checkpointWindows.add(3L);
    checkpoints = new HashMap<PTOperator, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 3L, cp);

    pnode2.checkpointWindows.add(4L);
    checkpoints = new HashMap<PTOperator, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 3L, cp);

    pnode1.checkpointWindows.add(1, 4L);
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[]{3L, 4L, 5L}));
    checkpoints = new HashMap<PTOperator, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 4L, cp);
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[]{4L, 5L}));

  }


}
