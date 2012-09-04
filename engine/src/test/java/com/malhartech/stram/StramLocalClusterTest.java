/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.ComponentContextPair;
import com.malhartech.dag.Node;
import com.malhartech.dag.NodeContext;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stream.HDFSOutputStream;

public class StramLocalClusterTest
{
  private static Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);

  @Test
  public void testLocalClusterInitShutdown() throws Exception
  {
    // create test topology
    Properties props = new Properties();

    // input adapter to ensure shutdown works on end of stream
    props.put("stram.stream.input1.classname", NumberGeneratorInputAdapter.class.getName());
    props.put("stram.stream.input1.outputNode", "node1");
    props.put("stram.stream.input1.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    props.put("stram.stream.output.classname", HDFSOutputStream.class.getName());
    props.put("stram.stream.output.inputNode", "node2");
    props.put("stram.stream.output.filepath", "target/" + StramLocalClusterTest.class.getName() + "-testSetupShutdown.out");
    props.put("stram.stream.output.append", "false");

    props.put("stram.stream.n1n2.inputNode", "node1");
    props.put("stram.stream.n1n2.outputNode", "node2");
    props.put("stram.stream.n1n2.template", "defaultstream");

    props.put("stram.node.node1.classname", TopologyBuilderTest.EchoNode.class.getName());
    props.put("stram.node.node1.myStringProperty", "myStringPropertyValue");

    props.put("stram.node.node2.classname", TopologyBuilderTest.EchoNode.class.getName());

    props.setProperty(Topology.STRAM_MAX_CONTAINERS, "2");

    TopologyBuilder tb = new TopologyBuilder(new Configuration());
    tb.addFromProperties(props);

    StramLocalCluster localCluster = new StramLocalCluster(tb.getTopology());
    localCluster.run();
  }

  @Ignore // we have a problem with windows randomly getting lost
  @Test
  public void testChildRecovery() throws Exception
  {
    NewTopologyBuilder tb = new NewTopologyBuilder();

    NodeDecl node1 = tb.addNode("node1", new EchoNode());
    NodeDecl node2 = tb.addNode("node2", new EchoNode());

    tb.addStream("n1n2").
      setSource(node1.getOutput(EchoNode.OUTPUT1)).
      addSink(node2.getInput(EchoNode.INPUT1));

    //tb.validate();

    Topology tplg = tb.getTopology();
    tplg.getConf().setInt(Topology.STRAM_WINDOW_SIZE_MILLIS, 0); // disable window generator
    tplg.getConf().setInt(Topology.STRAM_CHECKPOINT_INTERVAL_MILLIS, 0); // disable auto backup

    TestWindowGenerator wingen = new TestWindowGenerator();
    StramLocalCluster localCluster = new StramLocalCluster(tplg);
    localCluster.runAsync();

    LocalStramChild c0 = waitForContainer(localCluster, node1);
    Thread.sleep(1000);

    Map<String, ComponentContextPair<Node, NodeContext>> nodeMap = c0.getNodes();
    Assert.assertEquals("number nodes", 2, nodeMap.size());

    PTNode ptNode1 = localCluster.findByLogicalNode(node1);
    ComponentContextPair<Node, NodeContext> n1 = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1);

    LocalStramChild c2 = waitForContainer(localCluster, node2);
    Map<String, ComponentContextPair<Node, NodeContext>> c2NodeMap = c2.getNodes();
    Assert.assertEquals("number nodes downstream", 1, c2NodeMap.size());
    ComponentContextPair<Node, NodeContext> n2 = c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2);

    Assert.assertEquals("initial window id", 0, n1.context.getLastProcessedWindowId());
    wingen.tick(1);

    waitForWindow(n1.context, 1);
    backupNode(c0, n1.context);

    wingen.tick(1);

    waitForWindow(n2.context, 2);
    backupNode(c2, n2.context);

    wingen.tick(1);

    // move window forward and wait for nodes to reach,
    // to ensure backup in previous windows was processed
    wingen.tick(1);

    //waitForWindow(n1, 3);
    waitForWindow(n2.context, 3);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);

    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    // simulate node failure
    localCluster.failContainer(c0);

    // replacement container starts empty
    // nodes will deploy after downstream node was removed
    LocalStramChild c0Replaced = waitForContainer(localCluster, node1);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after init

    Assert.assertNotSame("old container", c0, c0Replaced);
    Assert.assertNotSame("old container", c0.getContainerId(), c0Replaced.getContainerId());

    // verify change in downstream container
    LOG.debug("triggering c2 heartbeat processing");
    StramChildAgent c2Agent = localCluster.getContainerAgent(c2);

    // wait for downstream re-deploy to complete
    while (c2Agent.hasPendingWork()) {
      Thread.sleep(500);
      c2.triggerHeartbeat();
      LOG.debug("Waiting for {} to complete pending work.", c2.getContainerId());
    }

    Assert.assertEquals("downstream nodes after redeploy " + c2.getNodes(), 1, c2.getNodes().size());
    // verify that the downstream node was replaced
    ComponentContextPair<Node, NodeContext> n2Replaced = c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2Replaced);
    Assert.assertNotSame("node2 redeployed", n2, n2Replaced);

    ComponentContextPair<Node, NodeContext> n1Replaced = nodeMap.get(ptNode1.id);
    Assert.assertNotNull(n1Replaced);
    Assert.assertEquals("initial window id", 1, n1Replaced.context.getLastProcessedWindowId());

    localCluster.shutdown();

  }

  /**
   * Wait until instance of node comes online in a container
   *
   * @param localCluster
   * @param nodeConf
   * @return
   * @throws InterruptedException
   */
  private LocalStramChild waitForContainer(StramLocalCluster localCluster, NodeDecl nodeDecl) throws InterruptedException
  {
    PTNode node = localCluster.findByLogicalNode(nodeDecl);
    Assert.assertNotNull("no node for " + nodeDecl, node);

    LocalStramChild container;
    while (true) {
      if (node.container.containerId != null) {
        if ((container = localCluster.getContainer(node.container.containerId)) != null) {
          if (container.getNodes().get(node.id) != null) {
            return container;
          }
        }
      }
      try {
        LOG.debug("Waiting for {} in container {}", node, node.container.containerId);
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
      }
    }
  }

  private void waitForWindow(NodeContext nodeCtx, long windowId) throws InterruptedException
  {
    while (nodeCtx.getLastProcessedWindowId() < windowId) {
      LOG.debug("Waiting for window {} at node {}", windowId, nodeCtx.getId());
      Thread.sleep(100);
    }
  }

  private void backupNode(StramChild c, NodeContext nodeCtx)
  {
    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(nodeCtx.getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.nodeRequests = Collections.singletonList(backupRequest);
    LOG.debug("Requesting backup {} {}", c.getContainerId(), nodeCtx);
    c.processHeartbeatResponse(rsp);
  }

  public static class TestWindowGenerator {
    private final ManualScheduledExecutorService mses = new ManualScheduledExecutorService(1);
    public final WindowGenerator wingen = new WindowGenerator(mses);
    public TestWindowGenerator() {

      Configuration config = new Configuration();
      config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0);
      config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);

      wingen.setup(config);
    }

    public void tick(long steps) {
      mses.tick(steps);
    }
  }
}
