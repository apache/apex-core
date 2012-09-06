/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.malhartech.stram.DNodeManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.TopologyDeployer.PTNode;
import com.malhartech.stram.conf.NewTopologyBuilder;
import com.malhartech.stram.conf.Topology;
import com.malhartech.stram.conf.Topology.NodeDecl;

public class TopologyDeployerTest {

  @Test
  public void testStaticPartitioning() {
    NewTopologyBuilder b = new NewTopologyBuilder();

    NodeDecl node1 = b.addNode("node1", GenericTestNode.class);
    NodeDecl node2 = b.addNode("node2", GenericTestNode.class);

    NodeDecl mergeNode = b.addNode("mergeNode", GenericTestNode.class);

    b.addStream("n1n2")
      .setSerDeClass(TestStaticPartitioningSerDe.class)
      .setSource(node1.getOutput(GenericTestNode.OUTPUT1))
      .addSink(node2.getInput(GenericTestNode.INPUT1));

    b.addStream("mergeStream")
      .setSource(node2.getOutput(GenericTestNode.OUTPUT1))
      .addSink(mergeNode.getInput(GenericTestNode.INPUT1));

    Topology tplg = b.getTopology();
    tplg.setMaxContainerCount(2);

    TopologyDeployer td = new TopologyDeployer(tplg);

    Assert.assertEquals("number of containers", 2, td.getContainers().size());
    NodeDecl node2Decl = tplg.getNode(node2.getId());
    Assert.assertEquals("number partition instances", TestStaticPartitioningSerDe.partitions.length, td.getNodes(node2Decl).size());
  }

  @Test
  public void testInline() {

    NewTopologyBuilder b = new NewTopologyBuilder();

    NodeDecl node1 = b.addNode("node1", GenericTestNode.class);
    NodeDecl node2 = b.addNode("node2", GenericTestNode.class);
    NodeDecl node3 = b.addNode("node3", GenericTestNode.class);

    NodeDecl notInlineNode = b.addNode("notInlineNode", GenericTestNode.class);
    // partNode has 2 inputs, inline must be ignored with partitioned input
    NodeDecl partNode = b.addNode("partNode", GenericTestNode.class);

    b.addStream("n1Output1")
      .setInline(true)
      .setSource(node1.getOutput(GenericTestNode.OUTPUT1))
      .addSink(node2.getInput(GenericTestNode.INPUT1))
      .addSink(node3.getInput(GenericTestNode.INPUT1))
      .addSink(partNode.getInput(GenericTestNode.INPUT1));

    b.addStream("n2Output1")
      .setInline(false)
      .setSource(node2.getOutput(GenericTestNode.OUTPUT1))
      .addSink(node3.getInput(GenericTestNode.INPUT2))
      .addSink(notInlineNode.getInput(GenericTestNode.INPUT1));

    b.addStream("n3Output1")
      .setSerDeClass(TestStaticPartitioningSerDe.class)
      .setSource(node3.getOutput(GenericTestNode.OUTPUT1))
      .addSink(partNode.getInput(GenericTestNode.INPUT2));

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
