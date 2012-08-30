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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.actors.threadpool.Arrays;

import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.StreamContext;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;

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
    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");

    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
                       NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_INLINE, "true");
    input1.addProperty("maxTuples", "1");

    node1.addInput(input1);

    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
    Assert.assertEquals("number required containers", 1, dnm.getNumRequiredContainers());

    String containerId = "container1";
    StreamingContainerContext cc = dnm.assignContainerForTest(containerId, InetSocketAddress.createUnresolved("localhost", 0));
    LocalStramChild container = new LocalStramChild(containerId, null);
    cc.setWindowSizeMillis(0); // disable window generator
    cc.setCheckpointDfsPath(testWorkDir.getPath());
    container.init(cc);

    Map<InputAdapter, StreamContext> inputAdapters = container.getInputAdapters();
    Assert.assertEquals("number input adapters", 1, inputAdapters.size());

    InputAdapter input = (InputAdapter)inputAdapters.keySet().toArray()[0];
    input.resetWindow(0, 1);
    input.beginWindow(1);

    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(cc.getNodes().get(0).getDnodeId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.setNodeRequests(Collections.singletonList(backupRequest));
    // TODO: ensure node is running and context set (startup timing)
    container.processHeartbeatResponse(rsp);

    input.endWindow(1);
    InternalNode node = container.getNodeMap().get(backupRequest.getNodeId());

    input.beginWindow(2);
    input.endWindow(2);
    if (node.getContext().getCurrentWindowId() < 2) {
      Thread.sleep(500);
    }
    Assert.assertEquals("node @ window 2", 2, node.getContext().getCurrentWindowId());

    File expectedFile = new File(testWorkDir, cc.getNodes().get(0).getDnodeId() + "/1");
    Assert.assertTrue("checkpoint file not found: " + expectedFile, expectedFile.exists() && expectedFile.isFile());

    LOG.debug("Shutdown container {}", container.getContainerId());
    container.shutdown();

  }

  @Test
  public void testRecoveryCheckpoint() throws Exception
  {
    TopologyBuilder b = new TopologyBuilder(new Configuration());

    NodeConf node1 = b.getOrAddNode("node1");

    NodeConf node2 = b.getOrAddNode("node2");
    StreamConf n1n2 = b.getOrAddStream("n1n2");


    StreamConf input1 = b.getOrAddStream("input1");
    input1.addProperty(TopologyBuilder.STREAM_CLASSNAME,
                       NumberGeneratorInputAdapter.class.getName());
    input1.addProperty(TopologyBuilder.STREAM_INLINE, "true");
    input1.addProperty("maxTuples", "1");

    node1.addInput(input1);
    node1.addOutput(n1n2);

    node2.addInput(n1n2);

    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    DNodeManager dnm = new DNodeManager(b);
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
