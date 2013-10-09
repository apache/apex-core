/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.PartitionableOperator.Partition;
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

  private static List<Boolean> partitionedStates = new ArrayList<Boolean>();

  public static class TestPartitionOperator extends BaseOperator {

    public static final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

      @Override
      public void process(Object tuple)
      {
      }

    };

    @Override
    public void setup(OperatorContext context)
    {
      boolean partitioned = context.isPartitioned();
      partitionedStates.add(partitioned);
    }

  }

  public static class TestInputOperator implements InputOperator {

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
    }

    @Override
    public void teardown()
    {
    }

  }

  @Test
  public void testCountPartitionedState() throws Exception {
    LogicalPlan dag = new LogicalPlan();

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setAttribute(op1, OperatorContext.INITIAL_PARTITION_COUNT, 2);

    dag.addStream("s1", i1.output, op1.input);

    partitionedStates.clear();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(2000);

    Assert.assertTrue("partitioned states not empty ", partitionedStates.size() > 0);

    for (boolean partitionedState : partitionedStates) {
      Assert.assertTrue("partitioned state", partitionedState);
    }
  }

  @Test
  public void testParallelPartitionedState() throws Exception {

    LogicalPlan dag = new LogicalPlan();

    TestInputOperator i1 = new TestInputOperator();
    dag.addOperator("i1", i1);

    TestPartitionOperator op1 = new TestPartitionOperator();
    dag.addOperator("op1", op1);

    dag.setInputPortAttribute(op1.input, PortContext.PARTITION_PARALLEL, true);

    dag.addStream("s1", i1.output, op1.input);

    partitionedStates.clear();

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(2000);

    Assert.assertTrue("partitioned states not empty ", partitionedStates.size() > 0);

    for (boolean partitionedState : partitionedStates) {
      Assert.assertTrue("partitioned state", partitionedState);
    }
  }

}
