/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

/**
 * 
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class OiOEndWindowTest
{
  public OiOEndWindowTest()
  {
  }

  public static class TestIhnputOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>();
    public static long threadId;

    @Override
    public void emitTuples()
    {
      throw new RuntimeException(new InterruptedException());
    }

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
      throw new RuntimeException(new InterruptedException());
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

  public static class FirstGenericOperator implements Operator
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
    public void beginWindow(long windowId)
    {

    }

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 1st generic operator");
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

  public static class SecondGenericOperator implements Operator
  {
    public static long endwindowCount;
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>() {
      @Override
      public void process(Number tuple)
      {

      }

    };

    @Override
    public void beginWindow(long windowId)
    {

    }

    @Override
    public void endWindow()
    {
      endwindowCount++;
      logger.info("in end window of 2nd generic operator");

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
  public void validateOiOImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    TestIhnputOperator io = lp.addOperator("Input Operator", new TestIhnputOperator());
    FirstGenericOperator go = lp.addOperator("First Generic Operator", new FirstGenericOperator());    
    SecondGenericOperator out = lp.addOperator("Second Generic Operator", new SecondGenericOperator());
    lp.getOperatorMeta("Second Generic Operator").getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    StreamMeta stream = lp.addStream("Stream", io.output, go.input);
    StreamMeta stream1 = lp.addStream("Stream1", go.output, out.input);

    /* This test makes sure that since they are ThreadLocal, they indeed share a thread */
    stream1.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    StramLocalCluster slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertEquals("End Window Count", FirstGenericOperator.endwindowCount, SecondGenericOperator.endwindowCount);
  }

  private static final Logger logger = LoggerFactory.getLogger(OiOEndWindowTest.class);
}