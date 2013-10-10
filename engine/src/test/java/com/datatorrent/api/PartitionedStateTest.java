/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.PartitionableOperator.Partition;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class PartitionedStateTest
{

  public static class TestPartitionOperator extends BaseOperator {

    public static PartitionedState partitionedState = new PartitionedState();

    public static final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

      @Override
      public void process(Object tuple)
      {
      }

    };

    @OutputPortFieldAnnotation(name="output", optional = true)
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

    @Override
    public void setup(OperatorContext context)
    {
      partitionedState.setStates(context);
    }

  }

  public static class TestOutputOperator extends BaseOperator {

    public static PartitionedState partitionedState = new PartitionedState();

    public static final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

      @Override
      public void process(Object tuple)
      {
      }

    };

    @Override
    public void setup(OperatorContext context)
    {
      partitionedState.setStates(context);
    }

  }

  public static class TestInputOperator implements InputOperator {

    public static PartitionedState partitionedState = new PartitionedState();

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

    @Override
    public void emitTuples()
    {
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
      System.out.println("Input partition " + context.isPartitioned());
      System.out.println("Input strict partition " + context.isStrictPartitioned());
      partitionedState.setStates(context);
    }

    @Override
    public void teardown()
    {
    }

  }

  private static class PartitionedState {
    public List<Boolean> partitionedStates = new ArrayList<Boolean>();
    public List<Boolean> strictPartitionedStates = new ArrayList<Boolean>();

    public void setStates(OperatorContext context) {
      partitionedStates.add(context.isPartitioned());
      strictPartitionedStates.add(context.isStrictPartitioned());
    }

    public void clearStates() {
      partitionedStates.clear();
      strictPartitionedStates.clear();
    }
  }

  @Test
  public void testCountPartitionedState() throws Exception {
    LogicalPlan dag = new LogicalPlan();

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

    TestInputOperator.partitionedState.clearStates();
    TestPartitionOperator.partitionedState.clearStates();
    TestOutputOperator.partitionedState.clearStates();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(4000);
    lc.shutdown();

    for (boolean partitioned : TestInputOperator.partitionedState.partitionedStates) {
      Assert.assertFalse("input operator partitioned", partitioned);
    }
    for (boolean partitioned : TestInputOperator.partitionedState.strictPartitionedStates) {
      Assert.assertFalse("input operator strict partitioned", partitioned);
    }

    for (boolean partitioned : TestPartitionOperator.partitionedState.partitionedStates) {
      Assert.assertTrue("partition operator partitioned", partitioned);
    }
    for (boolean partitioned : TestPartitionOperator.partitionedState.strictPartitionedStates) {
      Assert.assertTrue("partition operator strict partitioned", partitioned);
    }

    for (boolean partitioned : TestOutputOperator.partitionedState.partitionedStates) {
      Assert.assertTrue("output operator partitioned", partitioned);
    }
    for (boolean partitioned : TestPartitionOperator.partitionedState.strictPartitionedStates) {
      Assert.assertTrue("output operator strict partitioned", partitioned);
    }

  }

  @Test
  public void testParallelPartitionedState() throws Exception {

    LogicalPlan dag = new LogicalPlan();

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

    TestInputOperator.partitionedState.clearStates();
    TestPartitionOperator.partitionedState.clearStates();
    TestOutputOperator.partitionedState.clearStates();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(4000);
    lc.shutdown();

    for (boolean partitioned : TestInputOperator.partitionedState.partitionedStates) {
      Assert.assertTrue("input operator partitioned", partitioned);
    }
    for (boolean partitioned : TestInputOperator.partitionedState.strictPartitionedStates) {
      Assert.assertFalse("input operator strict partitioned", partitioned);
    }

    for (boolean partitioned : TestPartitionOperator.partitionedState.partitionedStates) {
      Assert.assertTrue("partition operator partitioned", partitioned);
    }
    for (boolean partitioned : TestPartitionOperator.partitionedState.strictPartitionedStates) {
      Assert.assertTrue("partition operator strict partitioned", partitioned);
    }

    for (boolean partitioned : TestOutputOperator.partitionedState.partitionedStates) {
      Assert.assertFalse("output operator partitioned", partitioned);
    }
    for (boolean partitioned : TestOutputOperator.partitionedState.strictPartitionedStates) {
      Assert.assertFalse("partition operator strict partitioned", partitioned);
    }

  }

}
