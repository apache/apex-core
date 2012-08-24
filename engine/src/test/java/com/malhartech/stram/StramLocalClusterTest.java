/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import static com.malhartech.stram.conf.TopologyBuilder.STRAM_WINDOW_SIZE_MILLIS;
import static com.malhartech.stram.conf.TopologyBuilder.STREAM_CLASSNAME;
import static com.malhartech.stram.conf.TopologyBuilder.STREAM_INLINE;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.InternalNode;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.stram.StramLocalCluster.LocalStramChild;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StramToNodeRequest.RequestType;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;
import com.malhartech.stram.conf.TopologyBuilder.StreamConf;
import com.malhartech.stream.AbstractInputAdapter;
import com.malhartech.stream.HDFSOutputStream;

public class StramLocalClusterTest {

  private static Logger LOG = LoggerFactory.getLogger(StramLocalClusterTest.class);
  
  @Test
  public void testLocalClusterInitShutdown() throws Exception {
    // create test topology
    Properties props = new Properties();

    // input adapter to ensure shutdown works on end of stream
    props.put("stram.stream.input1.classname", NumberGeneratorInputAdapter.class.getName());
    props.put("stram.stream.input1.outputNode", "node1");
    props.put("stram.stream.input1.maxTuples", "1");

    // fake output adapter - to be ignored when determine shutdown
    props.put("stram.stream.output.classname", HDFSOutputStream.class.getName());
    props.put("stram.stream.output.inputNode", "node2");
    props.put("stram.stream.output.filepath", "localclustertest-testSetupShutdown.out");
    props.put("stram.stream.output.append", "false");
    
    props.put("stram.stream.n1n2.inputNode", "node1");
    props.put("stram.stream.n1n2.outputNode", "node2");
    props.put("stram.stream.n1n2.template", "defaultstream");

    props.put("stram.node.node1.classname", TopologyBuilderTest.EchoNode.class.getName());
    props.put("stram.node.node1.myStringProperty", "myStringPropertyValue");

    props.put("stram.node.node2.classname", TopologyBuilderTest.EchoNode.class.getName());

    props.setProperty(TopologyBuilder.NUM_CONTAINERS, "2");

    TopologyBuilder tplg = new TopologyBuilder(new Configuration());
    tplg.addFromProperties(props);

    StramLocalCluster localCluster = new StramLocalCluster(tplg);
    localCluster.run();
  }
  
  //@Test
  public void testChildRecovery() throws Exception {

    TopologyBuilder tb = new TopologyBuilder();
    tb.getConf().setInt(STRAM_WINDOW_SIZE_MILLIS, 0); // disable window generator
    tb.getConf().setInt(TopologyBuilder.STRAM_CHECKPOINT_INTERVAL_MILLIS, 0); // disable auto backup

    StreamConf input1 = tb.getOrAddStream("input1");
    input1.addProperty(STREAM_CLASSNAME,
        LocalTestInputAdapter.class.getName());
    input1.addProperty(STREAM_INLINE, "true");

    StreamConf n1n2 = tb.getOrAddStream("n1n2");    
    
    NodeConf node1 = tb.getOrAddNode("node1");
    node1.addInput(input1);
    node1.addOutput(n1n2);
    
    NodeConf node2 = tb.getOrAddNode("node2");
    node2.addInput(n1n2);
    
    tb.validate();
    
    for (NodeConf nodeConf : tb.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    StramLocalCluster localCluster = new StramLocalCluster(tb);
    localCluster.runAsync();

    LocalStramChild c0 = waitForContainer(localCluster, node1);
    Thread.sleep(1000);
    
    List<InputAdapter> inputAdapters = c0.getInputAdapters();
    Assert.assertEquals("number input adapters", 1, inputAdapters.size());

    Map<String, InternalNode> nodeMap = c0.getNodeMap();
    Assert.assertEquals("number nodes", 2, nodeMap.size());

    // safer to lookup via topology deployer
    InternalNode n1 = nodeMap.get(localCluster.findByLogicalNode(node1).id);
    Assert.assertNotNull(n1);

    LocalStramChild c2 = waitForContainer(localCluster, node2);
    Map<String, InternalNode> c2NodeMap = c2.getNodeMap();
    Assert.assertEquals("number nodes", 1, c2NodeMap.size());
    InternalNode n2 = c2NodeMap.get(localCluster.findByLogicalNode(node2).id);
    Assert.assertNotNull(n2);
    
    LocalTestInputAdapter input = (LocalTestInputAdapter)inputAdapters.get(0);
    Assert.assertEquals("initial window id", 0, input.getContext().getStartingWindowId());
    input.resetWindow(0, 1);

    input.beginWindow(1);
    waitForWindow(n1, 1);
    backupNode(c0, n1);
    input.endWindow(1);
    
    input.beginWindow(2);
    waitForWindow(n2, 2);
    backupNode(c2, n2);
    input.endWindow(2);

    // move window forward and wait for nodes to reach,
    // to ensure backup in previous windows was processed
    input.beginWindow(3);
    input.endWindow(3);

    //waitForWindow(n1, 3);
    waitForWindow(n2, 3);

    // propagate checkpoints to master
    c0.triggerHeartbeat();
    // wait for heartbeat cycle to complete
    c0.waitForHeartbeat(5000);
 
    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);
    
    // simulate node failure 
    localCluster.failContainer(c0);

    // replacement container will start empty until downstream undeploy completes
    c2.triggerHeartbeat();
    c2.waitForHeartbeat(5000);

    // replacement container starts empty 
    // wait for downstream undeploy to complete
    LocalStramChild c0Replaced = waitForContainer(localCluster, node1);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after init
    
    Assert.assertNotSame("old container", c0, c0Replaced);
    Assert.assertNotSame("old container", c0.getContainerId(), c0Replaced.getContainerId());

    inputAdapters = c0Replaced.getInputAdapters();
    input = (LocalTestInputAdapter)inputAdapters.get(0);
    Assert.assertEquals("number input adapters", 1, inputAdapters.size());
    Assert.assertEquals("initial window id", 1, input.getContext().getStartingWindowId());

    localCluster.shutdown();
    
  }

  /**
   * Wait until instance of node comes online in a container
   * @param localCluster
   * @param nodeConf
   * @return
   * @throws InterruptedException
   */
  private LocalStramChild waitForContainer(StramLocalCluster localCluster, NodeConf nodeConf) throws InterruptedException {
    PTNode node = localCluster.findByLogicalNode(nodeConf);
    Assert.assertNotNull("no node for " + nodeConf, node);
    
    LocalStramChild container;
    while (true) {
      if (node.container.containerId != null) {
        if ((container = localCluster.getContainer(node.container.containerId)) != null) {
          if (container.getNodeMap().get(node.id) != null) {
            return container;
          }
        }
      }
      try {
        LOG.debug("Waiting for {} in container {}", node, node.container.containerId);
        Thread.sleep(500);
      } catch (InterruptedException e) {
      }
    }
  }

  private void waitForWindow(InternalNode node, long windowId) throws InterruptedException {
    while (node.getContext().getCurrentWindowId() < windowId) {
      LOG.debug("Waiting for node {} current window {}", node, windowId);
      Thread.sleep(100);
    }
  }
  
  
  private void backupNode(StramChild c, InternalNode node) {
    StramToNodeRequest backupRequest = new StramToNodeRequest();
    backupRequest.setNodeId(node.getContext().getId());
    backupRequest.setRequestType(RequestType.CHECKPOINT);
    ContainerHeartbeatResponse rsp = new ContainerHeartbeatResponse();
    rsp.setNodeRequests(Collections.singletonList(backupRequest));
    LOG.debug("Requesting backup {} {}", c.getContainerId(), node);
    c.processHeartbeatResponse(rsp);
  }
  
  public static class LocalTestInputAdapter extends AbstractInputAdapter {

    @Override
    public void setup(StreamConfiguration config) {
    }

    @Override
    public void activate() {
    }

    @Override
    public void teardown() {
    }
    
  }


}
