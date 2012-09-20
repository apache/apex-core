/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.GenericTestModule;
import com.malhartech.dag.Module;
import com.malhartech.dag.ModuleContext;
import com.malhartech.dag.NumberGeneratorInputModule;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingContainerUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.DAGDeployer.PTNode;
import com.malhartech.stram.conf.NewDAGBuilder;
import com.malhartech.stram.conf.DAG.Operator;
import com.malhartech.stream.StramTestSupport;
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
    NewDAGBuilder tb = new NewDAGBuilder();
    // node with no inputs will be connected to window generator
    tb.addOperator("node1", NumberGeneratorInputModule.class)
        .setProperty("maxTuples", "1");
    ModuleManager dnm = new ModuleManager(tb.getDAG());

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
    Assert.assertEquals("maxTupes", 1, ((NumberGeneratorInputModule)node).getMaxTuples());

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
    NewDAGBuilder b = new NewDAGBuilder();

    Operator node1 = b.addOperator("node1", GenericTestModule.class);
    Operator node2 = b.addOperator("node2", GenericTestModule.class);

    b.addStream("n1n2")
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node2.getInput(GenericTestModule.INPUT1));

    ModuleManager dnm = new ModuleManager(b.getDAG());
    DAGDeployer deployer = dnm.getTopologyDeployer();
    List<PTNode> nodes1 = deployer.getNodes(node1);
    Assert.assertNotNull(nodes1);
    Assert.assertEquals(1, nodes1.size());
    PTNode pnode1 = nodes1.get(0);

    List<PTNode> nodes2 = deployer.getNodes(node2);
    Assert.assertNotNull(nodes2);
    Assert.assertEquals(1, nodes2.size());
    PTNode pnode2 = nodes2.get(0);

    Map<PTNode, Long> checkpoints = new HashMap<PTNode, Long>();
    long cp = dnm.getRecoveryCheckpoint(pnode2, checkpoints);
    Assert.assertEquals("no checkpoints " + pnode2, 0, cp);

    cp = dnm.getRecoveryCheckpoint(pnode1, new HashMap<PTNode, Long>());
    Assert.assertEquals("no checkpoints " + pnode1, 0, cp);

    // adding checkpoints to upstream only does not move recovery checkpoint
    pnode1.checkpointWindows.add(3L);
    pnode1.checkpointWindows.add(5L);
    cp = dnm.getRecoveryCheckpoint(pnode1, new HashMap<PTNode, Long>());
    Assert.assertEquals("no checkpoints " + pnode1, 0L, cp);

    pnode2.checkpointWindows.add(3L);
    checkpoints = new HashMap<PTNode, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 3L, cp);

    pnode2.checkpointWindows.add(4L);
    checkpoints = new HashMap<PTNode, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 3L, cp);

    pnode1.checkpointWindows.add(1, 4L);
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[]{3L, 4L, 5L}));
    checkpoints = new HashMap<PTNode, Long>();
    cp = dnm.getRecoveryCheckpoint(pnode1, checkpoints);
    Assert.assertEquals("checkpoint pnode1", 4L, cp);
    Assert.assertEquals(pnode1.checkpointWindows, Arrays.asList(new Long[]{4L, 5L}));

  }


}
