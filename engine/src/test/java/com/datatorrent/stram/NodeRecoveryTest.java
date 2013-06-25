/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.engine.RecoverableInputOperator;
import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.CheckpointListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;

import java.io.IOException;
import java.util.HashSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class NodeRecoveryTest
{
  private static final Logger logger = LoggerFactory.getLogger(NodeRecoveryTest.class);
  static HashSet<Long> collection = new HashSet<Long>(20);

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

  public static class CollectorOperator extends BaseOperator implements CheckpointListener
  {
    private boolean simulateFailure;
    private long checkPointWindowId;

    public final transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
//        logger.debug("adding the tuple {}", Codec.getStringWindowId(tuple));
        collection.add(tuple);
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
    }

    @Override
    public void committed(long windowId)
    {
      logger.debug("committed window {} and checkPointWindowId {}", windowId, checkPointWindowId);
      if (simulateFailure && windowId > this.checkPointWindowId && this.checkPointWindowId > 0) {
        throw new RuntimeException("Failure Simulation from " + this + " checkpointWindowId=" + checkPointWindowId);
      }
    }

  }

@Test
  public void testInputOperatorRecovery() throws Exception
  {
    collection.clear();
    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(LogicalPlan.CONTAINERS_MAX_COUNT).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

  @Test
  public void testOperatorRecovery() throws Exception
  {
    collection.clear();
    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().attr(LogicalPlan.CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(LogicalPlan.CONTAINERS_MAX_COUNT).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

//    for (Long l: collection) {
//      logger.debug(Codec.getStringWindowId(l));
//    }
    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

@Test
  public void testInlineOperatorsRecovery() throws Exception
  {
    collection.clear();
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
    dag.addStream("connection", rip.output, cm.input).setInline(true);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

//    for (Long l: collection) {
//      logger.debug(Codec.getStringWindowId(l));
//    }
    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

}
