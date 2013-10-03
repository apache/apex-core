/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import javax.validation.ConstraintViolationException;
import javax.validation.ValidationException;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.GenericNodeTest.GenericOperator;
import com.datatorrent.stram.engine.ProcessingModeTests.CollectorOperator;
import com.datatorrent.stram.engine.RecoverableInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class OiOStreamTest
{
  public OiOStreamTest()
  {
  }

  //@Test
  public void validatePositiveOiO()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    RecoverableInputOperator inputOperator = plan.addOperator("IntegerGenerator", new RecoverableInputOperator());
    CollectorOperator outputOperator = plan.addOperator("IntegerCollector", new CollectorOperator());
    plan.addStream("PossibleOiO", inputOperator.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiO validation", true);
    }
    catch (ConstraintViolationException ex) {
      Assert.fail("OIO Single InputPort");
    }
    catch (ValidationException ex) {
      Assert.fail("OIO Single InputPort");
    }
  }

  //@Test
  public void validatePositiveOiOOptionalInput()
  {
    LogicalPlan plan = new LogicalPlan();
    RecoverableInputOperator inputOp1 = plan.addOperator("InputOperator1", new RecoverableInputOperator());
    GenericOperator genOp = plan.addOperator("GenericOperator", new GenericOperator());
    plan.addStream("OiO1", inputOp1.output, genOp.ip1).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiO validation", true);
    }
    catch (ConstraintViolationException ex) {
      Assert.fail("OiO Single Connected InputPort");
    }
    catch (ValidationException ex) {
      Assert.fail("OiO Single Connected InputPort");
    }
  }

  //@Test
  public void validateNegativeOiO()
  {
    LogicalPlan plan = new LogicalPlan();
    RecoverableInputOperator inputOp1 = plan.addOperator("InputOperator1", new RecoverableInputOperator());
    RecoverableInputOperator inputOp2 = plan.addOperator("InputOperator2", new RecoverableInputOperator());
    GenericOperator genOp = plan.addOperator("GenericOperator", new GenericOperator());
    StreamMeta oio1 = plan.addStream("OiO1", inputOp1.output, genOp.ip1).setLocality(Locality.THREAD_LOCAL);
    StreamMeta oio2 = plan.addStream("OiO2", inputOp2.output, genOp.ip2).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.fail("OIO Both InputPorts");
    }
    catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }
    catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }

    oio1.setLocality(null);
    try {
      plan.validate();
      Assert.fail("OIO First InputPort");
    }
    catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }
    catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }

    oio1.setLocality(Locality.THREAD_LOCAL);
    oio2.setLocality(null);
    try {
      plan.validate();
      Assert.fail("OIO Second InputPort");
    }
    catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }
    catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }
  }

  public static class ThreadIdValidatingInputOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<Long>();
    public static long threadId;

    @Override
    public void emitTuples()
    {
      assert (threadId == Thread.currentThread().getId());
    }

    @Override
    public void beginWindow(long windowId)
    {
      assert (threadId == Thread.currentThread().getId());
    }

    @Override
    public void endWindow()
    {
      assert (threadId == Thread.currentThread().getId());
      throw new RuntimeException(new InterruptedException());
    }

    @Override
    public void setup(OperatorContext context)
    {
      threadId = Thread.currentThread().getId();
    }

    @Override
    public void teardown()
    {
      assert (threadId == Thread.currentThread().getId());
    }

  }

  public static class ThreadIdValidatingGenericOperator implements Operator
  {
    public static long threadId;
    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
    {
      @Override
      public void process(Number tuple)
      {
        assert (threadId == Thread.currentThread().getId());
      }

    };

    @Override
    public void beginWindow(long windowId)
    {
      assert (threadId == Thread.currentThread().getId());
    }

    @Override
    public void endWindow()
    {
      assert (threadId == Thread.currentThread().getId());
    }

    @Override
    public void setup(OperatorContext context)
    {
      threadId = Thread.currentThread().getId();
    }

    @Override
    public void teardown()
    {
      assert (threadId == Thread.currentThread().getId());
    }

  }

  @Test
  public void validateOiOImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    ThreadIdValidatingInputOperator io = lp.addOperator("Input Operator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericOperator go = lp.addOperator("Output Operator", new ThreadIdValidatingGenericOperator());
    StreamMeta stream = lp.addStream("Stream", io.output, go.input);

    /* The first test makes sure that when they are not ThreadLocal they use different threads */
    lp.validate();
    StramLocalCluster slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertNotSame("Thread Id", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperator.threadId);

    /* This test makes sure that since they are ThreadLocal, they indeed share a thread */
    stream.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertSame("Thread Id", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperator.threadId);
  }

  private static final Logger logger = LoggerFactory.getLogger(OiOStreamTest.class);
}