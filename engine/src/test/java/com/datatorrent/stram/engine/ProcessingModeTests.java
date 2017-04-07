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
package com.datatorrent.stram.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.TestMeta;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 *
 */
public class ProcessingModeTests
{
  @Rule
  public TestMeta testMeta = new TestMeta();

  private LogicalPlan dag;

  ProcessingMode processingMode;
  int maxTuples = 30;

  public ProcessingModeTests(ProcessingMode pm)
  {
    processingMode = pm;
  }

  @Before
  public void setup() throws IOException
  {
    dag = StramTestSupport.createDAG(testMeta);
    StreamingContainer.eventloop.start();
  }

  @After
  public void teardown()
  {
    StreamingContainer.eventloop.stop();
  }

  public void testLinearInputOperatorRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);
    dag.getMeta(rip).getAttributes().put(OperatorContext.PROCESSING_MODE, processingMode);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    if (Operator.ProcessingMode.EXACTLY_ONCE.equals(processingMode)) {
      dag.getMeta(cm).getAttributes().put(OperatorContext.PROCESSING_MODE, Operator.ProcessingMode.AT_MOST_ONCE);
    }
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();
  }

  public void testLinearOperatorRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.getMeta(cm).getAttributes().put(OperatorContext.PROCESSING_MODE, processingMode);

    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();
  }

  public void testLinearInlineOperatorsRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.getMeta(cm).getAttributes().put(OperatorContext.PROCESSING_MODE, processingMode);

    dag.addStream("connection", rip.output, cm.input).setLocality(Locality.CONTAINER_LOCAL);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();
  }

  public static class CollectorOperator extends BaseOperator implements com.datatorrent.api.Operator.CheckpointListener
  {
    public static HashSet<Long> collection = new HashSet<>(20);
    public static ArrayList<Long> duplicates = new ArrayList<>();
    private boolean simulateFailure;
    private long checkPointWindowId;
    public final transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
        logger.debug("adding the tuple {}", Codec.getStringWindowId(tuple));
        if (collection.contains(tuple)) {
          duplicates.add(tuple);
        } else {
          collection.add(tuple);
        }
      }

    };

    /**
     * @param simulateFailure the simulateFailure to set
     */
    public void setSimulateFailure(boolean simulateFailure)
    {
      this.simulateFailure = simulateFailure;
    }

    @Override
    public void setup(OperatorContext context)
    {
      simulateFailure &= (checkPointWindowId == 0);
      logger.debug("simulateFailure = {}", simulateFailure);
    }

    @Override
    public void checkpointed(long windowId)
    {
      if (this.checkPointWindowId == 0) {
        this.checkPointWindowId = windowId;
      }

      logger.debug("{} checkpointed at {}", this, Codec.getStringWindowId(windowId));
    }

    @Override
    public void committed(long windowId)
    {
      logger.debug("{} committed at {}", this, Codec.getStringWindowId(windowId));
      if (simulateFailure && windowId > this.checkPointWindowId && this.checkPointWindowId > 0) {
        throw new RuntimeException("Failure Simulation from " + this + " checkpointWindowId=" + Codec.getStringWindowId(checkPointWindowId));
      }
    }

  }

  public static class MultiInputOperator implements Operator
  {
    public final transient MyInputPort input1 = new MyInputPort(100);
    public final transient MyInputPort input2 = new MyInputPort(200);
    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();

    public class MyInputPort extends DefaultInputPort<Integer>
    {
      private final int id;

      public MyInputPort(int id)
      {
        this.id = id;
      }

      @Override
      public void process(Integer t)
      {
        output.emit(id + t);
      }

    }

    @Override
    public void beginWindow(long l)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext t1)
    {
    }

    @Override
    public void teardown()
    {
    }

  }

  @SuppressWarnings("SleepWhileInLoop")
  public void testNonLinearOperatorRecovery() throws InterruptedException
  {
    final HashSet<Object> collection = new HashSet<>();
    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap map = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    map.put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 0);
    map.put(OperatorContext.PROCESSING_MODE, processingMode);

    final GenericNode node = new GenericNode(new MultiInputOperator(),
        new com.datatorrent.stram.engine.OperatorContext(1, "operator", map, null));
    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("input1", 1024);
    AbstractReservoir reservoir2 = AbstractReservoir.newReservoir("input1", 1024);
    node.connectInputPort("input1", reservoir1);
    node.connectInputPort("input2", reservoir2);
    node.connectOutputPort("output", new Sink<Object>()
    {
      @Override
      public void put(Object t)
      {
        if (collection.contains(t)) {
          throw new RuntimeException("Duplicate Found!");
        }

        collection.add(t);
      }

      @Override
      public int getCount(boolean bln)
      {
        return 0;
      }

    });

    final AtomicBoolean active = new AtomicBoolean(false);
    Thread thread = new Thread()
    {
      @Override
      public void run()
      {
        active.set(true);
        node.activate();
        node.run();
        node.deactivate();
      }

    };
    thread.start();

    for (int i = 0; i < 100 && !active.get(); i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 1));
    reservoir1.add(1);
    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 1));
    reservoir1.add(new EndWindowTuple(1));
    reservoir2.add(1);
    reservoir2.add(new EndWindowTuple(1));

    for (int i = 0; i < 100 && collection.size() < 4; i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 2));
    reservoir1.add(2);
    reservoir1.add(new EndWindowTuple(2));

    for (int i = 0; i < 100 && collection.size() < 6; i++) {
      sleep(5);
    }

    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 4));
    reservoir2.add(4);
    reservoir2.add(new EndWindowTuple((4)));

    for (int i = 0; i < 100 && collection.size() < 9; i++) {
      sleep(5);
    }

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 3));
    reservoir1.add(3);
    reservoir1.add(new EndWindowTuple(3));

    sleep(500);

    reservoir1.add(new Tuple(MessageType.BEGIN_WINDOW, 5));
    reservoir1.add(5);
    reservoir2.add(new Tuple(MessageType.BEGIN_WINDOW, 5));
    reservoir1.add(new EndWindowTuple(5));
    reservoir2.add(5);
    reservoir2.add(new EndWindowTuple(5));

    for (int i = 0; i < 100 && collection.size() < 14; i++) {
      sleep(5);
    }

    thread.interrupt();
    thread.join();

    /* lets make sure that we have all the tuples and nothing more */
    for (Object o : collection) {
      if (o instanceof Tuple) {
        Tuple t = (Tuple)o;
        long windowId = t.getWindowId();
        Assert.assertTrue("Valid Window Id", windowId == 1 || windowId == 2 || windowId == 4 || windowId == 5);
        Assert.assertTrue("Valid Tuple Type", t.getType() == MessageType.BEGIN_WINDOW || t.getType() == MessageType.END_WINDOW);
      } else {
        switch (((Integer)o).intValue()) {
          case 101:
          case 201:
          case 102:
          case 204:
          case 105:
          case 205:
            break;

          default:
            Assert.fail("Unexpected Data Tuple: " + o);
        }
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(ProcessingModeTests.class);
}
