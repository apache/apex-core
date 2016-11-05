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
package com.datatorrent.stram.plan.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.validation.ValidationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.DefaultDelayOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.StreamingContainerManager.UpdateCheckpointsContext;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan.OperatorMeta;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.plan.physical.PhysicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.MemoryStorageAgent;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for topologies with delay operator
 */
public class DelayOperatorTest
{
  @Rule
  public TestMeta testMeta = new TestMeta();

  private static Lock sequential = new ReentrantLock();

  @Before
  public void setup()
  {
    sequential.lock();
  }

  @After
  public void teardown()
  {
    sequential.unlock();
  }

  @Test
  public void testInvalidDelayDetection()
  {
    LogicalPlan dag = new LogicalPlan();

    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator<Object> opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToD", opDelay.output, opD.inport2);

    List<List<String>> invalidDelays = new ArrayList<>();
    dag.findInvalidDelays(dag.getMeta(opB), invalidDelays, new Stack<OperatorMeta>());
    assertEquals("operator invalid delay", 1, invalidDelays.size());

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }

    dag = new LogicalPlan();

    opB = dag.addOperator("B", GenericTestOperator.class);
    opC = dag.addOperator("C", GenericTestOperator.class);
    opD = dag.addOperator("D", GenericTestOperator.class);
    opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);
    dag.setOperatorAttribute(opDelay, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToC", opDelay.output, opC.inport2);

    invalidDelays = new ArrayList<>();
    dag.findInvalidDelays(dag.getMeta(opB), invalidDelays, new Stack<OperatorMeta>());
    assertEquals("operator invalid delay", 1, invalidDelays.size());

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }

    dag = new LogicalPlan();

    opB = dag.addOperator("B", GenericTestOperator.class);
    opC = dag.addOperator("C", GenericTestOperator.class);
    opD = dag.addOperator("D", GenericTestOperator.class);
    opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input).setLocality(DAG.Locality.THREAD_LOCAL);
    dag.addStream("DelayToC", opDelay.output, opC.inport2).setLocality(DAG.Locality.THREAD_LOCAL);

    try {
      dag.validate();
      fail("validation should fail");
    } catch (ValidationException e) {
      // expected
    }
  }

  @Test
  public void testValidDelay()
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator opA = dag.addOperator("A", TestGeneratorInputOperator.class);
    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("AtoB", opA.outport, opB.inport1);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToB", opDelay.output, opB.inport2);
    dag.validate();
  }

  public static final Long[] FIBONACCI_NUMBERS = new Long[]{
      1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L, 55L, 89L, 144L, 233L, 377L, 610L, 987L, 1597L, 2584L, 4181L, 6765L,
      10946L, 17711L, 28657L, 46368L, 75025L, 121393L, 196418L, 317811L, 514229L, 832040L, 1346269L, 2178309L,
      3524578L, 5702887L, 9227465L, 14930352L, 24157817L, 39088169L, 63245986L, 102334155L
  };

  public static class FibonacciOperator extends BaseOperator
  {
    public static List<Long> results = new ArrayList<>();
    public long currentNumber = 1;
    private transient long tempNum;

    public transient DefaultInputPort<Object> dummyInputPort = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };
    public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
        tempNum = tuple;
      }
    };
    public transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();

    @Override
    public void endWindow()
    {
      output.emit(currentNumber);
      results.add(currentNumber);
      currentNumber += tempNum;
      if (currentNumber <= 0) {
        // overflow
        currentNumber = 1;
      }
    }

  }

  public static class FailableFibonacciOperator extends FibonacciOperator implements Operator.CheckpointListener
  {
    private boolean committed = false;
    private int simulateFailureWindows = 0;
    private boolean simulateFailureAfterCommit = false;
    private int windowCount = 0;
    public static volatile boolean failureSimulated = false;

    @Override
    public void beginWindow(long windowId)
    {
      if (simulateFailureWindows > 0 && ((simulateFailureAfterCommit && committed) || !simulateFailureAfterCommit) &&
          !failureSimulated) {
        LOG.debug("FailableFibonacciOperator beginWindow {} {} {}", windowId, windowCount, simulateFailureWindows);
        if (windowCount++ == simulateFailureWindows) {
          failureSimulated = true;
          LOG.debug("FailableFibonacciOperator is simulating failure");
          throw new RuntimeException("simulating failure");
        }
      }
    }

    @Override
    public void checkpointed(long windowId)
    {
      LOG.debug("FailableFibonacciOperator is checkpointed {}", windowId);
    }

    @Override
    public void committed(long windowId)
    {
      LOG.debug("FailableFibonacciOperator is committed {}", windowId);
      committed = true;
    }

    public void setSimulateFailureWindows(int windows, boolean afterCommit)
    {
      this.simulateFailureAfterCommit = afterCommit;
      this.simulateFailureWindows = windows;
    }
  }

  public static class FailableDelayOperator extends DefaultDelayOperator implements Operator.CheckpointListener
  {
    private boolean committed = false;
    private int simulateFailureWindows = 0;
    private boolean simulateFailureAfterCommit = false;
    private int windowCount = 0;
    private static volatile boolean failureSimulated = false;

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      if (simulateFailureWindows > 0 && ((simulateFailureAfterCommit && committed) || !simulateFailureAfterCommit) &&
          !failureSimulated) {
        LOG.debug("FailableDelayOperator beginWindow {} {} {}", windowId, windowCount, simulateFailureWindows);
        if (windowCount++ == simulateFailureWindows) {
          failureSimulated = true;
          LOG.debug("FailableDelayOperator is simulating failure {}", windowId);
          throw new RuntimeException("simulating failure");
        }
      }
    }

    @Override
    public void checkpointed(long windowId)
    {
      LOG.debug("FailableDelayOperator is checkpointed {}", windowId);
    }

    @Override
    public void committed(long windowId)
    {
      LOG.debug("FailableDelayOperator is committed {}", windowId);
      committed = true;
    }

    public void setSimulateFailureWindows(int windows, boolean afterCommit)
    {
      this.simulateFailureAfterCommit = afterCommit;
      this.simulateFailureWindows = windows;
    }
  }


  @Test
  public void testFibonacci() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FibonacciOperator fib = dag.addOperator("FIB", FibonacciOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    FibonacciOperator.results.clear();
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FibonacciOperator.results.size() >= 10;
      }
    });
    localCluster.run(10000);
    Assert.assertArrayEquals(Arrays.copyOfRange(FIBONACCI_NUMBERS, 0, 10),
        FibonacciOperator.results.subList(0, 10).toArray());
  }

  @Test
  public void testFibonacciRecovery1() throws Exception
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FailableFibonacciOperator fib = dag.addOperator("FIB", FailableFibonacciOperator.class);
    DefaultDelayOperator opDelay = dag.addOperator("opDelay", DefaultDelayOperator.class);

    fib.setSimulateFailureWindows(3, true);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.HEARTBEAT_INTERVAL_MILLIS, 50);
    FailableFibonacciOperator.results.clear();
    FailableFibonacciOperator.failureSimulated = false;
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setPerContainerBufferServer(true);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FailableFibonacciOperator.results.size() >= 30;
      }
    });
    localCluster.run(60000);
    Assert.assertTrue("failure should be invoked", FailableFibonacciOperator.failureSimulated);
    Assert.assertArrayEquals(Arrays.copyOfRange(new TreeSet<>(Arrays.asList(FIBONACCI_NUMBERS)).toArray(), 0, 20),
        Arrays.copyOfRange(new TreeSet<>(FibonacciOperator.results).toArray(), 0, 20));
  }

  @Test
  public void testFibonacciRecovery2() throws Exception
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);

    TestGeneratorInputOperator dummyInput = dag.addOperator("DUMMY", TestGeneratorInputOperator.class);
    FibonacciOperator fib = dag.addOperator("FIB", FibonacciOperator.class);
    FailableDelayOperator opDelay = dag.addOperator("opDelay", FailableDelayOperator.class);

    opDelay.setSimulateFailureWindows(5, true);

    dag.addStream("dummy_to_operator", dummyInput.outport, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.HEARTBEAT_INTERVAL_MILLIS, 50);
    FibonacciOperator.results.clear();
    FailableDelayOperator.failureSimulated = false;
    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.setPerContainerBufferServer(true);
    localCluster.setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return FibonacciOperator.results.size() >= 30;
      }
    });
    localCluster.run(60000);

    Assert.assertTrue("failure should be invoked", FailableDelayOperator.failureSimulated);
    Assert.assertArrayEquals(Arrays.copyOfRange(new TreeSet<>(Arrays.asList(FIBONACCI_NUMBERS)).toArray(), 0, 20),
        Arrays.copyOfRange(new TreeSet<>(FibonacciOperator.results).toArray(), 0, 20));
  }

  @Test
  public void testCheckpointUpdate()
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);

    TestGeneratorInputOperator opA = dag.addOperator("A", TestGeneratorInputOperator.class);
    GenericTestOperator opB = dag.addOperator("B", GenericTestOperator.class);
    GenericTestOperator opC = dag.addOperator("C", GenericTestOperator.class);
    GenericTestOperator opD = dag.addOperator("D", GenericTestOperator.class);
    DefaultDelayOperator<Object> opDelay = dag.addOperator("opDelay", new DefaultDelayOperator<>());

    dag.addStream("AtoB", opA.outport, opB.inport1);
    dag.addStream("BtoC", opB.outport1, opC.inport1);
    dag.addStream("CtoD", opC.outport1, opD.inport1);
    dag.addStream("CtoDelay", opC.outport2, opDelay.input);
    dag.addStream("DelayToB", opDelay.output, opB.inport2);
    dag.validate();

    dag.setAttribute(com.datatorrent.api.Context.OperatorContext.STORAGE_AGENT, new MemoryStorageAgent());
    StreamingContainerManager scm = new StreamingContainerManager(dag);
    PhysicalPlan plan = scm.getPhysicalPlan();
    // set all operators as active to enable recovery window id update
    for (PTOperator oper : plan.getAllOperators().values()) {
      oper.setState(PTOperator.State.ACTIVE);
    }

    Clock clock = new SystemClock();

    PTOperator opA1 = plan.getOperators(dag.getMeta(opA)).get(0);
    PTOperator opB1 = plan.getOperators(dag.getMeta(opB)).get(0);
    PTOperator opC1 = plan.getOperators(dag.getMeta(opC)).get(0);
    PTOperator opDelay1 = plan.getOperators(dag.getMeta(opDelay)).get(0);
    PTOperator opD1 = plan.getOperators(dag.getMeta(opD)).get(0);

    Checkpoint cp3 = new Checkpoint(3L, 0, 0);
    Checkpoint cp5 = new Checkpoint(5L, 0, 0);
    Checkpoint cp4 = new Checkpoint(4L, 0, 0);

    opB1.checkpoints.add(cp3);
    opC1.checkpoints.add(cp3);
    opC1.checkpoints.add(cp4);
    opDelay1.checkpoints.add(cp3);
    opDelay1.checkpoints.add(cp5);
    opD1.checkpoints.add(cp5);
    // construct grouping that would be supplied through LogicalPlan
    Set<OperatorMeta> stronglyConnected = Sets.newHashSet(dag.getMeta(opB), dag.getMeta(opC), dag.getMeta(opDelay));
    Map<OperatorMeta, Set<OperatorMeta>> groups = new HashMap<>();
    for (OperatorMeta om : stronglyConnected) {
      groups.put(om, stronglyConnected);
    }

    UpdateCheckpointsContext ctx = new UpdateCheckpointsContext(clock, false, groups);
    scm.updateRecoveryCheckpoints(opB1, ctx, false);

    Assert.assertEquals("checkpoint " + opA1, Checkpoint.INITIAL_CHECKPOINT, opA1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + opB1, cp3, opC1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + opC1, cp3, opC1.getRecoveryCheckpoint());
    Assert.assertEquals("checkpoint " + opD1, cp5, opD1.getRecoveryCheckpoint());

  }

  @Test
  public void testValidationWithMultipleStreamLoops()
  {
    LogicalPlan dag = StramTestSupport.createDAG(testMeta);

    TestGeneratorInputOperator source = dag.addOperator("A", TestGeneratorInputOperator.class);
    GenericTestOperator op1 = dag.addOperator("Op1", GenericTestOperator.class);
    GenericTestOperator op2 = dag.addOperator("Op2", GenericTestOperator.class);
    DefaultDelayOperator<Object> delay = dag.addOperator("Delay", DefaultDelayOperator.class);

    dag.addStream("Source", source.outport, op1.inport1);
    dag.addStream("Stream1", op1.outport1, op2.inport1);
    dag.addStream("Stream2", op1.outport2, op2.inport2);
    dag.addStream("Op to Delay", op2.outport1, delay.input);
    dag.addStream("Delay to Op", delay.output, op1.inport2);

    dag.validate();
  }

  private static final Logger LOG = LoggerFactory.getLogger(DelayOperatorTest.class);

}
