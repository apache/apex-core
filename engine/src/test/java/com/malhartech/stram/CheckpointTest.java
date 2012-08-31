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

import com.malhartech.dag.ComponentContextPair;
import com.malhartech.dag.HeartbeatCounters;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeContext;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StramLocalClusterTest.TestWindowGenerator;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.Topology.NodeDecl;
import java.util.ArrayList;

/**
 *
 */
public class CheckpointTest {
  private static Logger LOG = LoggerFactory.getLogger(CheckpointTest.class);

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
    TestWindowGenerator wingen = new TestWindowGenerator();
    NewTopologyBuilder tb = new NewTopologyBuilder();
    // node with no inputs will be connected to window generator
    tb.addNode("node1", new NumberGeneratorInputAdapter())
        .setProperty("maxTuples", "1");
    DNodeManager dnm = new DNodeManager(tb.getTopology());

    Assert.assertEquals("number required containers", 1, dnm.getNumRequiredContainers());

    String containerId = "container1";
    StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", 0));
    LocalStramChild container = new LocalStramChild(containerId, null, wingen.wingen);
    cc.setCheckpointDfsPath(testWorkDir.getPath());
    container.init(cc);

    wingen.tick(1);

    Assert.assertEquals("number nodes", 1, container.getNodes().size());
    ComponentContextPair<Node, NodeContext> nodePair = container.getNodes().get(cc.nodeList.get(0).id);

    Assert.assertNotNull("node deployed " + cc.nodeList.get(0), nodePair);
    Assert.assertEquals("nodeId", cc.nodeList.get(0).id, nodePair.context.getId());
    Assert.assertEquals("maxTupes", 1, ((NumberGeneratorInputAdapter)nodePair.component).getMaxTuples());

    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(nodePair.context.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.setNodeRequests(Collections.singletonList(backupRequest));
    container.processHeartbeatResponse(rsp);


    wingen.tick(1);

    // node to move to next window before we verify the checkpoint state
    if (nodePair.context.getLastProcessedWindowId() < 2) {
      Thread.sleep(500);
    }

    Assert.assertEquals("node @ window 2", 2, nodePair.context.getLastProcessedWindowId());

    File expectedFile = new File(testWorkDir, backupRequest.getNodeId() + "/1");
    Assert.assertTrue("checkpoint file not found: " + expectedFile, expectedFile.exists() && expectedFile.isFile());

    LOG.debug("Shutdown container {}", container.getContainerId());
    container.shutdown();

  }

  @Test
  public void testRecoveryCheckpoint() throws Exception
  {
    NewTopologyBuilder b = new NewTopologyBuilder();

    NodeDecl node1 = b.addNode("node1", new EchoNode());
    NodeDecl node2 = b.addNode("node2", new EchoNode());

    b.addStream("n1n2")
      .setSource(node1.getOutput(EchoNode.OUTPUT1))
      .addSink(node2.getInput(EchoNode.INPUT1));

    DNodeManager dnm = new DNodeManager(b.getTopology());
    TopologyDeployer deployer = dnm.getTopologyDeployer();
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
