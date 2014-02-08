/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.StramChildAgent;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.api.OperatorDeployInfo;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTContainer;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

/**
 *
 * @author Pramod Immaneni <pramod@datatorrent.com>
 */
public class OutputUnifiedTest
{

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  public static class TestPartitionOperator extends BaseOperator {

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

      @Override
      public void process(Object tuple)
      {
        output.emit(tuple);
      }

    };

    private static class TestUnifier implements Unifier<Object>{

      public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

      @Override
      public void process(Object tuple)
      {
        output.emit(tuple);
      }

      @Override
      public void beginWindow(long windowId)
      {
      }

      @Override
      public void endWindow()
      {
      }

      @Override
      public void setup(OperatorContext context)
      {
      }

      @Override
      public void teardown()
      {
      }

    }

    @OutputPortFieldAnnotation(name="output", optional = true)
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>() {

      @Override
      public Unifier<Object> getUnifier()
      {
        return new TestUnifier();
      }

      @Override
      public void setup(PortContext context)
      {
      }

    };

  }

  public static class TestOutputOperator extends BaseOperator {

    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

      @Override
      public void process(Object tuple)
      {
      }

    };

  }

  public static class TestInputOperator implements InputOperator {


    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>() {

      @Override
      public void setup(PortContext context)
      {
      }

    };

    @Override
    public void emitTuples()
    {
      output.emit(new Object());
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

  }

  @Test
  public void testManyToOnePartition() throws Exception {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.INITIAL_PARTITION_COUNT, 3);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.input);
    dag.addStream("s2", op1.output, op2.input);

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
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.INITIAL_PARTITION_COUNT, 3);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.setAttribute(op2, OperatorContext.INITIAL_PARTITION_COUNT, 2);

    dag.addStream("s1", i1.output, op1.input);
    dag.addStream("s2", op1.output, op2.input);

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
    dag.setAttribute(DAGContext.APPLICATION_PATH, testMeta.dir);

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    dag.setAttribute(i1, OperatorContext.INITIAL_PARTITION_COUNT, 2);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setInputPortAttribute(op1.input, PortContext.PARTITION_PARALLEL, true);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.input);
    dag.addStream("s2", op1.output, op2.input);

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
      StramChildAgent agent = scm.getContainerAgent("container" + container.getId());
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

  private static StramChildAgent assignContainer(StreamingContainerManager scm, String containerId) {
    return scm.assignContainer(new StreamingContainerManager.ContainerResource(0, containerId, "localhost", 1024, null), InetSocketAddress.createUnresolved(containerId+"Host", 0));
  }

}
