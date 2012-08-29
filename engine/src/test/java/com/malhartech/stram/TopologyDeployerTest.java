/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.TopologyBuilderTest.EchoNode;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.NodeDecl;
import com.malhartech.stram.conf.TopologyBuilder;
import com.malhartech.stram.conf.TopologyBuilder.NodeConf;

public class TopologyDeployerTest {

  @Test
  public void testStaticPartitioning() {
    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");

    NodeConf mergeNode = b.getOrAddNode("mergeNode");
    
    b.getOrAddStream("n1n2")
      .addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName())
      .setSource(EchoNode.OUTPUT1, node1)
      .addSink(EchoNode.INPUT1, node2);

    b.getOrAddStream("mergeStream")
      .setSource(EchoNode.OUTPUT1, node2)
      .addSink(EchoNode.INPUT1, mergeNode);
    
    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }

    Topology tplg = b.getTopology();
    tplg.setMaxContainerCount(2);
    
    TopologyDeployer td = new TopologyDeployer(tplg);
    
    Assert.assertEquals("number of containers", 2, td.getContainers().size());
    NodeDecl node2Decl = tplg.getNode(node2.getId());
    Assert.assertEquals("number partition instances", TestStaticPartitioningSerDe.partitions.length, td.getNodes(node2Decl).size());
  }  

  @Test
  public void testInline() {
    
    TopologyBuilder b = new TopologyBuilder(new Configuration());
    
    NodeConf node1 = b.getOrAddNode("node1");
    NodeConf node2 = b.getOrAddNode("node2");
    NodeConf node3 = b.getOrAddNode("node3");
    
    NodeConf notInlineNode = b.getOrAddNode("notInlineNode");
    // partNode has 2 inputs, inline must be ignored with partitioned input
    NodeConf partNode = b.getOrAddNode("partNode");

    for (NodeConf nodeConf : b.getAllNodes().values()) {
      nodeConf.setClassName(TopologyBuilderTest.EchoNode.class.getName());
    }
    
    b.getOrAddStream("n1Output1")
      .addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(true))
      .setSource(EchoNode.OUTPUT1, node1)
      .addSink(EchoNode.INPUT1, node2)
      .addSink(EchoNode.INPUT1, node3)
      .addSink(EchoNode.INPUT1, partNode);

    b.getOrAddStream("n2Output1")
      .addProperty(TopologyBuilder.STREAM_INLINE, String.valueOf(false))
      .setSource(EchoNode.OUTPUT1, node2)
      .addSink(EchoNode.INPUT2, node3)
      .addSink(EchoNode.INPUT1, notInlineNode);
    
    b.getOrAddStream("n3Output1")
      .addProperty(TopologyBuilder.STREAM_SERDE_CLASSNAME, TestStaticPartitioningSerDe.class.getName())
      .setSource(EchoNode.OUTPUT1, node3)
      .addSink(EchoNode.INPUT2, partNode);

    int maxContainers = 5;
    Topology tplg = b.getTopology();
    tplg.setMaxContainerCount(maxContainers);
    TopologyDeployer deployer1 = new TopologyDeployer(tplg);
    Assert.assertEquals("number of containers", maxContainers, deployer1.getContainers().size());
    Assert.assertEquals("nodes container 0", 3, deployer1.getContainers().get(0).nodes.size());

    List<NodeDecl> c1ExpNodes = Arrays.asList(tplg.getNode(node1.getId()), tplg.getNode(node2.getId()), tplg.getNode(node3.getId()));
    List<NodeDecl> c1ActNodes = new ArrayList<NodeDecl>();
    for (PTNode pNode : deployer1.getContainers().get(0).nodes) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("nodes container 0", c1ExpNodes, c1ActNodes);
    
    Assert.assertEquals("nodes container 1", 1, deployer1.getContainers().get(1).nodes.size());
    Assert.assertEquals("nodes container 1", tplg.getNode(notInlineNode.getId()), deployer1.getContainers().get(1).nodes.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("nodes container" + cindex, 1, deployer1.getContainers().get(cindex).nodes.size());
      Assert.assertEquals("nodes container" + cindex, tplg.getNode(partNode.getId()), deployer1.getContainers().get(cindex).nodes.get(0).getLogicalNode());
    }
    
  }  
  
}
