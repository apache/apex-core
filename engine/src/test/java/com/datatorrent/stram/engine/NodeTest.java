/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class NodeTest
{
  static class TestGenericOperator implements Operator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  static class TestInputOperator implements InputOperator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void emitTuples()
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  public NodeTest()
  {
  }

  @Ignore
  @Test
  public void testStreamingWindowGenericNode() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 10);
    dag.addOperator("GenericOperator", new TestGenericOperator());

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(2000);



  }

  @Test
  public void testApplicationWindowGenericNode()
  {
  }

  @Test
  public void testStreamingWindowInputNode()
  {
  }

  @Test
  public void testApplicationWindowInputNode()
  {
  }

}
