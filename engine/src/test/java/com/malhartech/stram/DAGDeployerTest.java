/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.GenericTestModule;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.malhartech.stram.ModuleManagerTest.TestStaticPartitioningSerDe;
import com.malhartech.stram.DAGDeployer.PTNode;
import com.malhartech.stram.DAGDeployer.PTOutput;
import com.malhartech.stram.conf.NewDAGBuilder;
import com.malhartech.stram.conf.DAG;
import com.malhartech.stram.conf.DAG.Operator;

public class DAGDeployerTest {

  @Test
  public void testStaticPartitioning() {
    NewDAGBuilder b = new NewDAGBuilder();

    Operator node1 = b.addNode("node1", GenericTestModule.class);
    Operator node2 = b.addNode("node2", GenericTestModule.class);

    Operator mergeNode = b.addNode("mergeNode", GenericTestModule.class);

    b.addStream("n1n2")
      .setSerDeClass(TestStaticPartitioningSerDe.class)
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node2.getInput(GenericTestModule.INPUT1));

    b.addStream("mergeStream")
      .setSource(node2.getOutput(GenericTestModule.OUTPUT1))
      .addSink(mergeNode.getInput(GenericTestModule.INPUT1));

    DAG tplg = b.getTopology();
    tplg.setMaxContainerCount(2);

    DAGDeployer td = new DAGDeployer(tplg);

    Assert.assertEquals("number of containers", 2, td.getContainers().size());
    Operator node2Decl = tplg.getOperator(node2.getId());
    Assert.assertEquals("number partition instances", TestStaticPartitioningSerDe.partitions.length, td.getNodes(node2Decl).size());
  }

  @Test
  public void testInline() {

    NewDAGBuilder b = new NewDAGBuilder();

    Operator node1 = b.addNode("node1", GenericTestModule.class);
    Operator node2 = b.addNode("node2", GenericTestModule.class);
    Operator node3 = b.addNode("node3", GenericTestModule.class);

    Operator notInlineNode = b.addNode("notInlineNode", GenericTestModule.class);
    // partNode has 2 inputs, inline must be ignored with partitioned input
    Operator partNode = b.addNode("partNode", GenericTestModule.class);

    b.addStream("n1Output1")
      .setInline(true)
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node2.getInput(GenericTestModule.INPUT1))
      .addSink(node3.getInput(GenericTestModule.INPUT1))
      .addSink(partNode.getInput(GenericTestModule.INPUT1));

    b.addStream("n2Output1")
      .setInline(false)
      .setSource(node2.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node3.getInput(GenericTestModule.INPUT2))
      .addSink(notInlineNode.getInput(GenericTestModule.INPUT1));

    b.addStream("n3Output1")
      .setSerDeClass(TestStaticPartitioningSerDe.class)
      .setSource(node3.getOutput(GenericTestModule.OUTPUT1))
      .addSink(partNode.getInput(GenericTestModule.INPUT2));

    int maxContainers = 5;
    DAG tplg = b.getTopology();
    tplg.setMaxContainerCount(maxContainers);
    DAGDeployer deployer1 = new DAGDeployer(tplg);
    Assert.assertEquals("number of containers", maxContainers, deployer1.getContainers().size());
    Assert.assertEquals("nodes container 0", 3, deployer1.getContainers().get(0).nodes.size());

    Set<Operator> c1ExpNodes = Sets.newHashSet(tplg.getOperator(node1.getId()), tplg.getOperator(node2.getId()), tplg.getOperator(node3.getId()));
    Set<Operator> c1ActNodes = new HashSet<Operator>();
    for (PTNode pNode : deployer1.getContainers().get(0).nodes) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("nodes container 0", c1ExpNodes, c1ActNodes);

    Assert.assertEquals("nodes container 1", 1, deployer1.getContainers().get(1).nodes.size());
    Assert.assertEquals("nodes container 1", tplg.getOperator(notInlineNode.getId()), deployer1.getContainers().get(1).nodes.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("nodes container" + cindex, 1, deployer1.getContainers().get(cindex).nodes.size());
      Assert.assertEquals("nodes container" + cindex, tplg.getOperator(partNode.getId()), deployer1.getContainers().get(cindex).nodes.get(0).getLogicalNode());
    }

  }

  @Test
  public void testInlineMultipleInputs() {

    NewDAGBuilder b = new NewDAGBuilder();

    Operator node1 = b.addNode("node1", GenericTestModule.class);
    Operator node2 = b.addNode("node2", GenericTestModule.class);
    Operator node3 = b.addNode("node3", GenericTestModule.class);

    b.addStream("n1Output1")
      .setInline(true)
      .setSource(node1.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node3.getInput(GenericTestModule.INPUT1));

    b.addStream("n2Output1")
      .setInline(true)
      .setSource(node2.getOutput(GenericTestModule.OUTPUT1))
      .addSink(node3.getInput(GenericTestModule.INPUT2));

    int maxContainers = 5;
    DAG tplg = b.getTopology();
    tplg.setMaxContainerCount(maxContainers);
    DAGDeployer deployer = new DAGDeployer(tplg);
    Assert.assertEquals("number of containers", 1, deployer.getContainers().size());

    PTOutput node1Out = deployer.getNodes(node1).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node1Out, deployer.isDownStreamInline(node1Out));

    // per current logic, different container is assigned to second input node
    PTOutput node2Out = deployer.getNodes(node2).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node2Out, deployer.isDownStreamInline(node2Out));

  }

}
