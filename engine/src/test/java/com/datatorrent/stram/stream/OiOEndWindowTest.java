/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import com.datatorrent.api.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

/**
 *
 * @author Gaurav Gupta <gaurav@datatorrent.com>
 */
public class OiOEndWindowTest
{
  public OiOEndWindowTest()
  {
  }

  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>();

    @Override
    public void emitTuples()
    {
      BaseOperator.shutdown();
    }

  }

  public static class FirstGenericOperator extends BaseOperator
  {
    public static long endwindowCount;
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>();
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>() {
      @Override
      public void process(Number tuple)
      {
      }
    };

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 1st generic operator");
    }

  }

  public static class SecondGenericOperator extends BaseOperator
  {
    public static long endwindowCount;
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>() {
      @Override
      public void process(Number tuple)
      {
      }

    };

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 2nd generic operator");

    }

  }

  @Test
  public void validateOiOImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    TestInputOperator io = lp.addOperator("Input Operator", new TestInputOperator());
    FirstGenericOperator go = lp.addOperator("First Generic Operator", new FirstGenericOperator());
    SecondGenericOperator out = lp.addOperator("Second Generic Operator", new SecondGenericOperator());

    /*
     * This tests make sure that even if the application_window_count is different the endWindow() is called for
     * end_stream
     */
    lp.getOperatorMeta("Second Generic Operator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    StreamMeta stream = lp.addStream("Stream", io.output, go.input);
    StreamMeta stream1 = lp.addStream("Stream1", go.output, out.input);

    stream1.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    StramLocalCluster slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertEquals("End Window Count", FirstGenericOperator.endwindowCount, SecondGenericOperator.endwindowCount);
  }

  private static final Logger logger = LoggerFactory.getLogger(OiOEndWindowTest.class);
}