/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.malhartech.api.DAG;
import com.malhartech.api.DAG.OperatorWrapper;
import com.malhartech.dag.GenericTestModule;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.PhysicalPlan.PTOutput;
import com.malhartech.stram.StreamingContainerManagerTest.TestStaticPartitioningSerDe;

public class PhysicalPlanTest {

  @Test
  public void testStaticPartitioning() {
    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);

    GenericTestModule mergeNode = dag.addOperator("mergeNode", GenericTestModule.class);

    dag.addStream("n1n2", node1.outport1, node2.inport1)
      .setSerDeClass(TestStaticPartitioningSerDe.class);

    dag.addStream("mergeStream", node2.outport1, mergeNode.inport1);

    dag.setMaxContainerCount(2);

    PhysicalPlan plan = new PhysicalPlan(dag);

    Assert.assertEquals("number of containers", 2, plan.getContainers().size());
    OperatorWrapper node2Decl = dag.getOperatorWrapper(node2.getName());
    Assert.assertEquals("number partition instances", TestStaticPartitioningSerDe.partitions.length, plan.getOperators(node2Decl).size());
  }

  @Test
  public void testInline() {

    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    GenericTestModule notInlineNode = dag.addOperator("notInlineNode", GenericTestModule.class);
    // partNode has 2 inputs, inline must be ignored with partitioned input
    GenericTestModule partNode = dag.addOperator("partNode", GenericTestModule.class);

    dag.addStream("n1Output1", node1.outport1, node2.inport1, node3.inport1, partNode.inport1)
      .setInline(true);

    dag.addStream("n2Output1", node2.outport1, node3.inport2, notInlineNode.inport1)
      .setInline(false);

    dag.addStream("n3Output1", node3.outport1, partNode.inport2)
      .setSerDeClass(TestStaticPartitioningSerDe.class);

    int maxContainers = 5;
    dag.setMaxContainerCount(maxContainers);
    PhysicalPlan deployer1 = new PhysicalPlan(dag);
    Assert.assertEquals("number of containers", maxContainers, deployer1.getContainers().size());
    Assert.assertEquals("operators container 0", 3, deployer1.getContainers().get(0).operators.size());

    Set<OperatorWrapper> c1ExpNodes = Sets.newHashSet(dag.getOperatorWrapper(node1.getName()), dag.getOperatorWrapper(node2.getName()), dag.getOperatorWrapper(node3.getName()));
    Set<OperatorWrapper> c1ActNodes = new HashSet<OperatorWrapper>();
    for (PTOperator pNode : deployer1.getContainers().get(0).operators) {
      c1ActNodes.add(pNode.getLogicalNode());
    }
    Assert.assertEquals("operators container 0", c1ExpNodes, c1ActNodes);

    Assert.assertEquals("operators container 1", 1, deployer1.getContainers().get(1).operators.size());
    Assert.assertEquals("operators container 1", dag.getOperatorWrapper(notInlineNode.getName()), deployer1.getContainers().get(1).operators.get(0).getLogicalNode());

    // one container per partition
    for (int cindex = 2; cindex < maxContainers; cindex++) {
      Assert.assertEquals("operators container" + cindex, 1, deployer1.getContainers().get(cindex).operators.size());
      Assert.assertEquals("operators container" + cindex, dag.getOperatorWrapper(partNode.getName()), deployer1.getContainers().get(cindex).operators.get(0).getLogicalNode());
    }

  }

  @Test
  public void testInlineMultipleInputs() {

    DAG dag = new DAG();

    GenericTestModule node1 = dag.addOperator("node1", GenericTestModule.class);
    GenericTestModule node2 = dag.addOperator("node2", GenericTestModule.class);
    GenericTestModule node3 = dag.addOperator("node3", GenericTestModule.class);

    dag.addStream("n1Output1", node1.outport1, node3.inport1)
      .setInline(true);

    dag.addStream("n2Output1", node2.outport1, node3.inport2)
      .setInline(true);

    int maxContainers = 5;
    dag.setMaxContainerCount(maxContainers);

    PhysicalPlan deployer = new PhysicalPlan(dag);
    Assert.assertEquals("number of containers", 1, deployer.getContainers().size());

    PTOutput node1Out = deployer.getOperators(dag.getOperatorWrapper(node1)).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node1Out, deployer.isDownStreamInline(node1Out));

    // per current logic, different container is assigned to second input node
    PTOutput node2Out = deployer.getOperators(dag.getOperatorWrapper(node2)).get(0).outputs.get(0);
    Assert.assertTrue("inline " + node2Out, deployer.isDownStreamInline(node2Out));

  }

}
