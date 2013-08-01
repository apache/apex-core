/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.IOException;
import static java.lang.Thread.sleep;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.engine.DefaultReservoir;
import com.datatorrent.engine.GenericNode;
import com.datatorrent.engine.RecoverableInputOperator;
import com.datatorrent.stram.NodeRecoveryTest.CollectorOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.tuple.EndWindowTuple;
import com.datatorrent.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AtMostOnceTest
{
  @Before
  public void setup() throws IOException
  {
    StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    StramChild.eventloop.stop();
  }

  @Test
  public void testLinearInputOperatorRecovery() throws Exception
  {
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(LogicalPlan.CONTAINERS_MAX_COUNT).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    dag.getMeta(rip).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertTrue("Generated Outputs", maxTuples <= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  public void testLinearOperatorRecovery() throws Exception
  {
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(LogicalPlan.CONTAINERS_MAX_COUNT).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.getMeta(cm).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertTrue("Generated Outputs", maxTuples >= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  @Test
  public void testLinearInlineOperatorsRecovery() throws Exception
  {
    CollectorOperator.collection.clear();
    CollectorOperator.duplicates.clear();

    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    //dag.getAttributes().attr(DAG.HEARTBEAT_INTERVAL_MILLIS).set(400);
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(LogicalPlan.CONTAINERS_MAX_COUNT).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.getMeta(cm).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    dag.addStream("connection", rip.output, cm.input).setInline(true);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertTrue("Generated Outputs", maxTuples >= CollectorOperator.collection.size());
    Assert.assertTrue("No Duplicates", CollectorOperator.duplicates.isEmpty());
  }

  public static class MultiInputOperator implements Operator
  {
    public final transient MyInputPort input1 = new MyInputPort(100);
    public final transient MyInputPort input2 = new MyInputPort(200);
    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

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

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNonLinearOperatorRecovery() throws InterruptedException
  {
    final HashSet<Object> collection = new HashSet<Object>();

    final GenericNode node = new GenericNode(new MultiInputOperator());
    DefaultReservoir reservoir1 = new DefaultReservoir("input1", 1024);
    DefaultReservoir reservoir2 = new DefaultReservoir("input1", 1024);
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
        AttributeMap.DefaultAttributeMap map = new AttributeMap.DefaultAttributeMap(OperatorContext.class);
        map.attr(OperatorContext.CHECKPOINT_WINDOW_COUNT).set(0);
        map.attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);
        active.set(true);
        node.activate(new com.datatorrent.engine.OperatorContext(1, this, map, null));
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

    /* lets make sure that we have all the tuples and nothing more */
    for (Object o : collection) {
      if (o instanceof Tuple) {
        Tuple t = (Tuple)o;
        long windowId = t.getWindowId();
        Assert.assertTrue("Valid Window Id", windowId == 1 || windowId == 2 || windowId == 4 || windowId == 5);
        Assert.assertTrue("Valid Tuple Type", t.getType() == MessageType.BEGIN_WINDOW || t.getType() == MessageType.END_WINDOW);
      }
      else {
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

  private static final Logger logger = LoggerFactory.getLogger(AtMostOnceTest.class);
}
