/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.GenericNodeTest.GenericOperator;
import com.datatorrent.stram.engine.ProcessingModeTests.CollectorOperator;
import com.datatorrent.stram.engine.RecoverableInputOperator;
import com.datatorrent.stram.plan.TestPlanContext;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlan.StreamMeta;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;

/**
 *
 */
public class OiOStreamTest
{
  public OiOStreamTest()
  {
  }

  @Test
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
    } catch (ConstraintViolationException ex) {
      Assert.fail("OIO Single InputPort");
    }
  }

  @Test
  public void validatePositiveOiOiO()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator = plan.addOperator("intermediateOperator", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingOutputOperator outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingOutputOperator());

    plan.addStream("OiO1", inputOperator.output, intermediateOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiO2", intermediateOperator.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiOiO validation", true);
    } catch (ConstraintViolationException ex) {
      Assert.fail("OiOiO validation");
    }
  }

  @Test
  public void validatePositiveOiOOptionalInput()
  {
    LogicalPlan plan = new LogicalPlan();
    RecoverableInputOperator inputOp1 = plan.addOperator("InputOperator1", new RecoverableInputOperator());
    GenericOperator genOp = plan.addOperator("GenericOperator", new GenericOperator());
    plan.addStream("OiO1", inputOp1.output, genOp.ip1).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiO validation", true);
    } catch (ConstraintViolationException ex) {
      Assert.fail("OiO Single Connected InputPort");
    }
  }

  @Test
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
    } catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    } catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }

    oio1.setLocality(null);
    try {
      plan.validate();
      Assert.fail("OIO First InputPort");
    } catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    } catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }

    oio1.setLocality(Locality.THREAD_LOCAL);
    oio2.setLocality(null);
    try {
      plan.validate();
      Assert.fail("OIO Second InputPort");
    } catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    } catch (ValidationException ex) {
      Assert.assertTrue("OiO validation passed", true);
    }
  }

  @Test
  public void validatePositiveOiOiOdiamond()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = plan.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = plan.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    plan.addStream("OiOin", inputOperator.output, intermediateOperator1.input, intermediateOperator2.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout1", intermediateOperator1.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout2", intermediateOperator2.output, outputOperator.input2).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiOiO diamond validation", true);
    } catch (ConstraintViolationException ex) {
      Assert.fail("OIOIO diamond validation");
    }
  }

  @Test
  public void validatePositiveOiOiOdiamondWithCores()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = plan.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = plan.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator3 = plan.addOperator("intermediateOperator3", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator4 = plan.addOperator("intermediateOperator4", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    plan.addStream("OiOin", inputOperator.output, intermediateOperator1.input, intermediateOperator3.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOIntermediate1", intermediateOperator1.output, intermediateOperator2.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOIntermediate2", intermediateOperator3.output, intermediateOperator4.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout1", intermediateOperator2.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout2", intermediateOperator4.output, outputOperator.input2).setLocality(Locality.THREAD_LOCAL);

    plan.setOperatorAttribute(inputOperator, OperatorContext.VCORES, 1);
    plan.setOperatorAttribute(intermediateOperator1, OperatorContext.VCORES, 1);
    plan.setOperatorAttribute(intermediateOperator2, OperatorContext.VCORES, 2);
    plan.setOperatorAttribute(intermediateOperator3, OperatorContext.VCORES, 3);
    plan.setOperatorAttribute(intermediateOperator4, OperatorContext.VCORES, 5);
    plan.setAttribute(OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    try {
      plan.validate();
      Assert.assertTrue("OiOiO extended diamond validation", true);
    } catch (ConstraintViolationException ex) {
      Assert.fail("OIOIO extended diamond validation");
    }
    PhysicalPlan physicalPlan = new PhysicalPlan(plan, new TestPlanContext());
    Assert.assertTrue("number of containers", 1 == physicalPlan.getContainers().size());
    Assert.assertTrue("number of vcores " + physicalPlan.getContainers().get(0).getRequiredVCores(), 5 == physicalPlan.getContainers().get(0).getRequiredVCores());
  }


  @Test
  public void validateNegativeOiOiOdiamond()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = plan.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = plan.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    plan.addStream("OiOin", inputOperator.output, intermediateOperator1.input, intermediateOperator2.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout1", intermediateOperator1.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("nonOiOout2", intermediateOperator2.output, outputOperator.input2).setLocality(null);

    try {
      plan.validate();
      Assert.fail("OIOIO negative diamond");
    } catch (ConstraintViolationException ex) {
      Assert.assertTrue("OIOIO negative diamond", true);
    } catch (ValidationException ex) {
      Assert.assertTrue("OIOIO negative diamond", true);
    }
  }

  @Test
  public void validatePositiveOiOiOExtendeddiamond()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = plan.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = plan.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator3 = plan.addOperator("intermediateOperator3", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator4 = plan.addOperator("intermediateOperator4", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    plan.addStream("OiOin", inputOperator.output, intermediateOperator1.input, intermediateOperator3.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOIntermediate1", intermediateOperator1.output, intermediateOperator2.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOIntermediate2", intermediateOperator3.output, intermediateOperator4.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout1", intermediateOperator2.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout2", intermediateOperator4.output, outputOperator.input2).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.assertTrue("OiOiO extended diamond validation", true);
    } catch (ConstraintViolationException ex) {
      Assert.fail("OIOIO extended diamond validation");
    }
  }

  @Test
  public void validateNegativeOiOiOExtendeddiamond()
  {
    logger.info("Checking the logic for sanity checking of OiO");

    LogicalPlan plan = new LogicalPlan();
    ThreadIdValidatingInputOperator inputOperator = plan.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = plan.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = plan.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator3 = plan.addOperator("intermediateOperator3", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator4 = plan.addOperator("intermediateOperator4", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = plan.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    plan.addStream("OiOin", inputOperator.output, intermediateOperator1.input, intermediateOperator3.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOIntermediate1", intermediateOperator1.output, intermediateOperator2.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("nonOiOIntermediate2", intermediateOperator3.output, intermediateOperator4.input).setLocality(null);
    plan.addStream("OiOout1", intermediateOperator2.output, outputOperator.input).setLocality(Locality.THREAD_LOCAL);
    plan.addStream("OiOout2", intermediateOperator4.output, outputOperator.input2).setLocality(Locality.THREAD_LOCAL);

    try {
      plan.validate();
      Assert.fail("OiOiO extended diamond validation");
    } catch (ConstraintViolationException ex) {
      Assert.assertTrue("OiOiO extended diamond validation", true);
    } catch (ValidationException ex) {
      Assert.assertTrue("OiOiO extended diamond validation", true);
    }
  }

  public static class ThreadIdValidatingInputOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();
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
      BaseOperator.shutdown();
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

  public static class ThreadIdValidatingOutputOperator implements Operator
  {
    public static long threadId;
    public static List<Long> threadList = Collections.synchronizedList(new ArrayList<Long>());

    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
    {
      @Override
      public void process(Number tuple)
      {
        assert (threadList.contains(Thread.currentThread().getId()));
      }

    };

    @Override
    public void beginWindow(long windowId)
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

    @Override
    public void endWindow()
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

    @Override
    public void setup(OperatorContext context)
    {
      threadId = Thread.currentThread().getId();
      threadList.add(Thread.currentThread().getId());
    }

    @Override
    public void teardown()
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

  }

  public static class ThreadIdValidatingGenericIntermediateOperator implements Operator
  {
    public static long threadId;
    public static List<Long> threadList = Collections.synchronizedList(new ArrayList<Long>());

    public final transient DefaultInputPort<Number> input = new DefaultInputPort<Number>()
    {
      @Override
      public void process(Number tuple)
      {
        assert (threadList.contains(Thread.currentThread().getId()));
      }

    };

    @Override
    public void beginWindow(long windowId)
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

    @Override
    public void endWindow()
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

    @Override
    public void setup(OperatorContext context)
    {
      threadId = Thread.currentThread().getId();
      threadList.add(Thread.currentThread().getId());
    }

    @Override
    public void teardown()
    {
      assert (threadList.contains(Thread.currentThread().getId()));
    }

    public final transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();
  }

  public static class ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts extends ThreadIdValidatingGenericIntermediateOperator
  {
    public final transient DefaultOutputPort<Long> output2 = new DefaultOutputPort<>();
  }

  public static class ThreadIdValidatingGenericOperatorWithTwoInputPorts implements Operator
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

    public final transient DefaultInputPort<Number> input2 = new DefaultInputPort<Number>()
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
    lp.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ThreadIdValidatingInputOperator io = lp.addOperator("Input Operator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingOutputOperator go = lp.addOperator("Output Operator", new ThreadIdValidatingOutputOperator());
    StreamMeta stream = lp.addStream("Stream", io.output, go.input);

    /* The first test makes sure that when they are not ThreadLocal they use different threads */
    ThreadIdValidatingOutputOperator.threadList.clear();
    lp.validate();
    StramLocalCluster slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertFalse("Thread Id", ThreadIdValidatingInputOperator.threadId == ThreadIdValidatingOutputOperator.threadId);

    /* This test makes sure that since they are ThreadLocal, they indeed share a thread */
    ThreadIdValidatingOutputOperator.threadList.clear();
    stream.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertEquals("Thread Id", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingOutputOperator.threadId);
  }

  @Test
  public void validateOiOiOImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ThreadIdValidatingInputOperator inputOperator = lp.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator = lp.addOperator("intermediateOperator", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingOutputOperator outputOperator = lp.addOperator("outputOperator", new ThreadIdValidatingOutputOperator());

    StreamMeta stream1 = lp.addStream("OiO1", inputOperator.output, intermediateOperator.input);
    StreamMeta stream2 = lp.addStream("OiO2", intermediateOperator.output, outputOperator.input);

    StramLocalCluster slc;

    /* The first test makes sure that when they are not ThreadLocal they use different threads */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertFalse("Thread Id 1", ThreadIdValidatingInputOperator.threadId == ThreadIdValidatingGenericIntermediateOperator.threadId);
    Assert.assertFalse("Thread Id 2", ThreadIdValidatingGenericIntermediateOperator.threadId == ThreadIdValidatingOutputOperator.threadId);

    /* This test makes sure that since they are ThreadLocal, they indeed share a thread */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    stream1.setLocality(Locality.THREAD_LOCAL);
    stream2.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();
    Assert.assertEquals("Thread Id 3", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericIntermediateOperator.threadId);
    Assert.assertEquals("Thread Id 4", ThreadIdValidatingGenericIntermediateOperator.threadId, ThreadIdValidatingOutputOperator.threadId);
  }

  @Test
  public void validateOiOiODiamondImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ThreadIdValidatingInputOperator inputOperator = lp.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator1 = lp.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperator2 = lp.addOperator("intermediateOperator2", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = lp.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    StreamMeta stream1 = lp.addStream("OiOinput", inputOperator.output, intermediateOperator1.input, intermediateOperator2.input);
    StreamMeta stream2 = lp.addStream("OiOintermediateToOutput1", intermediateOperator1.output, outputOperator.input);
    StreamMeta stream3 = lp.addStream("OiOintermediateToOutput2", intermediateOperator2.output, outputOperator.input2);

    StramLocalCluster slc;

    /*
     * The first test makes sure that when they are not ThreadLocal they use different threads
     */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("nonOIO: Number of threads", 2, ThreadIdValidatingGenericIntermediateOperator.threadList.size());
    Assert.assertFalse("nonOIO: Thread Ids of input operator and intermediate operator1",
        ThreadIdValidatingInputOperator.threadId == ThreadIdValidatingGenericIntermediateOperator.threadList.get(0));
    Assert.assertFalse("nonOIO: Thread Ids of input operator and intermediate operator2",
        ThreadIdValidatingInputOperator.threadId == ThreadIdValidatingGenericIntermediateOperator.threadList.get(1));
    Assert.assertNotEquals("nonOIO: Thread Ids of two intermediate operators", ThreadIdValidatingGenericIntermediateOperator.threadList.get(0), ThreadIdValidatingGenericIntermediateOperator.threadList.get(1));
    Assert.assertNotEquals("nonOIO: Thread Ids of input and output operators", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);

    /*
     * This test makes sure that since all operators in diamond are ThreadLocal, they indeed share a thread
     */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();
    stream1.setLocality(Locality.THREAD_LOCAL);
    stream2.setLocality(Locality.THREAD_LOCAL);
    stream3.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("OIO: Number of threads", 2, ThreadIdValidatingGenericIntermediateOperator.threadList.size());
    Assert.assertEquals("OIO: Thread Ids of input operator and intermediate operator1",
        ThreadIdValidatingInputOperator.threadId, (long)ThreadIdValidatingGenericIntermediateOperator.threadList.get(0));
    Assert.assertEquals("OIO: Thread Ids of input operator and intermediate operator2",
        ThreadIdValidatingInputOperator.threadId, (long)ThreadIdValidatingGenericIntermediateOperator.threadList.get(1));
    Assert.assertEquals("OIO: Thread Ids of two intermediate operators", ThreadIdValidatingGenericIntermediateOperator.threadList.get(0), ThreadIdValidatingGenericIntermediateOperator.threadList.get(1));
    Assert.assertEquals("OIO: Thread Ids of input and output operators", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);
  }

  @Test
  public void validateOiOiOTreeImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ThreadIdValidatingInputOperator inputOperator1 = lp.addOperator("inputOperator1", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperatorfromInputOper1 = lp.addOperator("intermediateOperatorfromInputOper1", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperatorfromInterOper11 = lp.addOperator("intermediateOperatorfromInterOper11", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingGenericIntermediateOperator intermediateOperatorfromInterOper12 = lp.addOperator("intermediateOperatorfromInterOper12", new ThreadIdValidatingGenericIntermediateOperator());
    ThreadIdValidatingOutputOperator outputOperatorFromInputOper = lp.addOperator("outputOperatorFromInputOper", new ThreadIdValidatingOutputOperator());
    ThreadIdValidatingOutputOperator outputOperatorFromInterOper11 = lp.addOperator("outputOperatorFromInterOper11", new ThreadIdValidatingOutputOperator());
    ThreadIdValidatingOutputOperator outputOperatorFromInterOper21 = lp.addOperator("outputOperatorFromInterOper21", new ThreadIdValidatingOutputOperator());
    ThreadIdValidatingOutputOperator outputOperatorFromInterOper22 = lp.addOperator("outputOperatorFromInterOper22", new ThreadIdValidatingOutputOperator());

    StreamMeta stream1 = lp.addStream("OiO1", inputOperator1.output, outputOperatorFromInputOper.input, intermediateOperatorfromInputOper1.input);
    StreamMeta stream2 = lp.addStream("OiO2", intermediateOperatorfromInputOper1.output, intermediateOperatorfromInterOper11.input, intermediateOperatorfromInterOper12.input);
    StreamMeta stream3 = lp.addStream("OiO3", intermediateOperatorfromInterOper11.output, outputOperatorFromInterOper11.input);
    lp.addStream("nonOiO1", intermediateOperatorfromInterOper12.output, outputOperatorFromInterOper21.input, outputOperatorFromInterOper22.input);

    StramLocalCluster slc;

    /*
     * This test makes sure that since no operators in dag tree are ThreadLocal, they dont share threads
     */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();

    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("nonOIO: Number of threads ThreadIdValidatingGenericIntermediateOperator", 3, ThreadIdValidatingGenericIntermediateOperator.threadList.size());
    Assert.assertEquals("nonOIO: Number of unique threads ThreadIdValidatingGenericIntermediateOperator", 3, (new HashSet<>(ThreadIdValidatingGenericIntermediateOperator.threadList)).size());
    Assert.assertEquals("nonOIO: Number of threads ThreadIdValidatingOutputOperator", 4, ThreadIdValidatingOutputOperator.threadList.size());
    Assert.assertEquals("nonOIO: Number of unique threads ThreadIdValidatingOutputOperator", 4, (new HashSet<>(ThreadIdValidatingOutputOperator.threadList)).size());
    Assert.assertFalse("nonOIO:: inputOperator1 : ThreadIdValidatingOutputOperator", ThreadIdValidatingOutputOperator.threadList.contains(ThreadIdValidatingInputOperator.threadId));
    Assert.assertFalse("nonOIO:: inputOperator1 : ThreadIdValidatingGenericIntermediateOperator", ThreadIdValidatingGenericIntermediateOperator.threadList.contains(ThreadIdValidatingInputOperator.threadId));

    /*
     * This test makes sure that since some operators in the dag tree are ThreadLocal, they indeed share a thread
     */
    ThreadIdValidatingGenericIntermediateOperator.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();
    stream1.setLocality(Locality.THREAD_LOCAL);
    stream2.setLocality(Locality.THREAD_LOCAL);
    stream3.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("OIO: Number of threads ThreadIdValidatingGenericIntermediateOperator", 3, ThreadIdValidatingGenericIntermediateOperator.threadList.size());
    Assert.assertEquals("OIO: Number of unique threads ThreadIdValidatingGenericIntermediateOperator", 1, (new HashSet<>(ThreadIdValidatingGenericIntermediateOperator.threadList)).size());
    Assert.assertEquals("OIO: Number of threads ThreadIdValidatingOutputOperator", 4, ThreadIdValidatingOutputOperator.threadList.size());
    Assert.assertEquals("OIO: Number of unique threads ThreadIdValidatingOutputOperator", 3, (new HashSet<>(ThreadIdValidatingOutputOperator.threadList)).size());
    Assert.assertTrue("OIO:: inputOperator1 : ThreadIdValidatingOutputOperator", ThreadIdValidatingOutputOperator.threadList.contains(ThreadIdValidatingInputOperator.threadId));
    Assert.assertTrue("OIO:: inputOperator1 : ThreadIdValidatingGenericIntermediateOperator", ThreadIdValidatingGenericIntermediateOperator.threadList.contains(ThreadIdValidatingInputOperator.threadId));
  }

  @Test
  public void validateOiOTwoPortBetweenOperatorsImplementation() throws Exception
  {
    LogicalPlan lp = new LogicalPlan();
    lp.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());

    ThreadIdValidatingInputOperator inputOperator = lp.addOperator("inputOperator", new ThreadIdValidatingInputOperator());
    ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts intermediateOperator = lp.addOperator("intermediateOperator1", new ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts());
    ThreadIdValidatingGenericOperatorWithTwoInputPorts outputOperator = lp.addOperator("outputOperator", new ThreadIdValidatingGenericOperatorWithTwoInputPorts());

    StreamMeta stream1 = lp.addStream("OiOinput", inputOperator.output, intermediateOperator.input);
    StreamMeta stream2 = lp.addStream("OiOintermediateOutput1", intermediateOperator.output, outputOperator.input);
    StreamMeta stream3 = lp.addStream("OiOintermediateOutput2", intermediateOperator.output2, outputOperator.input2);

    StramLocalCluster slc;

    /*
     * The first test makes sure that when they are not ThreadLocal they use different threads
     */
    ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("nonOIO: Number of threads", 1, ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadList.size());
    Assert.assertNotEquals("nonOIO: Thread Ids of input operator and intermediate operator",
        ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadId);
    Assert.assertNotEquals("nonOIO: Thread Ids of intermediate and output operators", ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);
    Assert.assertNotEquals("nonOIO: Thread Ids of input and output operators", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);

    /*
     * This test makes sure that since all streams between two operators are thread local, they indeed share a thread
     */
    ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadList.clear();
    ThreadIdValidatingOutputOperator.threadList.clear();
    stream2.setLocality(Locality.THREAD_LOCAL);
    stream3.setLocality(Locality.THREAD_LOCAL);
    lp.validate();
    slc = new StramLocalCluster(lp);
    slc.run();

    Assert.assertEquals("OIO: Number of threads", 1, ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadList.size());
    Assert.assertNotEquals("OIO: Thread Ids of input operator and intermediate operator",
        ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadId);
    Assert.assertEquals("OIO: Thread Ids of intermediate and output operators", ThreadIdValidatingGenericIntermediateOperatorWithTwoOutputPorts.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);
    Assert.assertNotEquals("OIO: Thread Ids of input and output operators", ThreadIdValidatingInputOperator.threadId, ThreadIdValidatingGenericOperatorWithTwoInputPorts.threadId);
  }

  private static final Logger logger = LoggerFactory.getLogger(OiOStreamTest.class);
}
