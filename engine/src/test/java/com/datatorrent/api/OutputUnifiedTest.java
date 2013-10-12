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
 * @author Pramod Immaneni <pramod@datatorrent.com>
 */
public class OutputUnifiedTest
{

  public static class TestPartitionOperator extends BaseOperator {

    public static List<Boolean> unifiedStates = new ArrayList<Boolean>();

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
        unifiedStates.add(context.getAttributes().attr(PortContext.IS_OUTPUT_UNIFIED).get());
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

    public static List<Boolean> unifiedStates = new ArrayList<Boolean>();

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>() {

      @Override
      public void setup(PortContext context)
      {
        unifiedStates.add(context.getAttributes().attr(PortContext.IS_OUTPUT_UNIFIED).get());
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

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.INITIAL_PARTITION_COUNT, 3);

    TestOutputOperator op2 = new TestOutputOperator();
    dag.addOperator("op2", op2);

    dag.addStream("s1", i1.output, op1.input);
    dag.addStream("s2", op1.output, op2.input);

    TestInputOperator.unifiedStates.clear();
    TestPartitionOperator.unifiedStates.clear();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(4000);
    lc.shutdown();

    for (boolean unified : TestInputOperator.unifiedStates) {
      Assert.assertFalse("partition operator output unified", unified);
    }
    for (boolean unified : TestPartitionOperator.unifiedStates) {
      Assert.assertTrue("partition operator output unified", unified);
    }

  }

  @Test
  public void testMxNPartition() throws Exception {
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

    TestInputOperator.unifiedStates.clear();
    TestPartitionOperator.unifiedStates.clear();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(4000);
    lc.shutdown();

    for (boolean unified : TestInputOperator.unifiedStates) {
      Assert.assertFalse("partition operator output unified", unified);
    }
    for (boolean unified : TestPartitionOperator.unifiedStates) {
      Assert.assertTrue("partition operator output unified", unified);
    }

  }

  @Test
  public void testParallelPartition() throws Exception {
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

    TestInputOperator.unifiedStates.clear();
    TestPartitionOperator.unifiedStates.clear();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(4000);
    lc.shutdown();

    for (boolean unified : TestInputOperator.unifiedStates) {
      Assert.assertFalse("partition operator output unified", unified);
    }
    for (boolean unified : TestPartitionOperator.unifiedStates) {
      Assert.assertTrue("partition operator output unified", unified);
    }

  }

}
