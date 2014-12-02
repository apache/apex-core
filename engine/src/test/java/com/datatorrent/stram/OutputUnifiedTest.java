/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.partitioner.StatelessPartitioner;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestOutputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.stream.OiOEndWindowTest.TestInputOperator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 */
public class OutputUnifiedTest
{

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Test
  public void testManyToOnePartition() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, testMeta.dir);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 6, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);

  }

  @Test
  public void testMxNPartition() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, testMeta.dir);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(3));

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.setAttribute(op2, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 6, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);

  }

  @Test
  public void testParallelPartition() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(com.datatorrent.api.Context.DAGContext.APPLICATION_PATH, testMeta.dir);
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    dag.setAttribute(i1, OperatorContext.PARTITIONER, new StatelessPartitioner<GenericTestOperator>(2));

    GenericTestOperator op1 = new GenericTestOperator();
    dag.addOperator("op1", op1);

    dag.setInputPortAttribute(op1.inport1, PortContext.PARTITION_PARALLEL, true);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.inport1);
    dag.addStream("s2", op1.outport1, op2.inport);

    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan physicalPlan = scm.getPhysicalPlan();
    List<PTContainer> containers = physicalPlan.getContainers();
    Assert.assertEquals("Number of containers", 6, containers.size());

    assignContainers(scm, containers);

    testOutputAttribute(dag, i1, scm, physicalPlan, false);
    testOutputAttribute(dag, op1, scm, physicalPlan, true);
  }

  private void testOutputAttribute(LogicalPlan dag, Operator operator, StreamingContainerManager scm, PhysicalPlan physicalPlan, boolean result)
  {
    List<PTOperator> ptOperators = physicalPlan.getOperators(dag.getMeta(operator));
    for (PTOperator ptOperator : ptOperators) {
      PTContainer container = ptOperator.getContainer();
      StreamingContainerAgent agent = scm.getContainerAgent("container" + container.getId());
      System.out.println("Opsize " + container.getOperators().size());
      List<OperatorDeployInfo> deployInfoList = agent.getDeployInfoList(container.getOperators());
      Assert.assertEquals("Deploy info size", 1, deployInfoList.size());
      Assert.assertEquals("Is output unified", deployInfoList.get(0).outputs.get(0).getAttributes().get(PortContext.IS_OUTPUT_UNIFIED), result);
    }
  }

  private void assignContainers(StreamingContainerManager scm, List<PTContainer> containers)
  {
    for (PTContainer container : containers) {
      assignContainer(scm, "container" + container.getId());
    }
  }

  private static StreamingContainerAgent assignContainer(StreamingContainerManager scm, String containerId) {
    return scm.assignContainer(new StreamingContainerManager.ContainerResource(0, containerId, "localhost", 1024, null), InetSocketAddress.createUnresolved(containerId+"Host", 0));
  }

}
