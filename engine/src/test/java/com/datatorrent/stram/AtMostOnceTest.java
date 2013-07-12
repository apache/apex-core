/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.engine.RecoverableInputOperator;
import com.datatorrent.stram.NodeRecoveryTest.CollectorOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AtMostOnceTest
{
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
    dag.getMeta(rip).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

  //@Test
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
    dag.getMeta(cm).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

//    for (Long l: collection) {
//      logger.debug(Codec.getStringWindowId(l));
//    }
    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

  //@Test
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
    dag.getMeta(cm).getAttributes().attr(OperatorContext.PROCESSING_MODE).set(ProcessingMode.AT_MOST_ONCE);

    dag.addStream("connection", rip.output, cm.input).setInline(true);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

  private static final Logger logger = LoggerFactory.getLogger(AtMostOnceTest.class);
}
