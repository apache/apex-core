/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.stram.StramChild;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.ProcessingModeTests.CollectorOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class AtLeastOnceTest
{
  private static final Logger logger = LoggerFactory.getLogger(AtLeastOnceTest.class);

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
    CollectorOperator.collection.clear();
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

    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
  }

  @Test
  public void testOperatorRecovery() throws Exception
  {
    CollectorOperator.collection.clear();
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
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
  }

  @Test
  public void testInlineOperatorsRecovery() throws Exception
  {
    CollectorOperator.collection.clear();
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
    dag.addStream("connection", rip.output, cm.input).setLocality(Locality.CONTAINER_LOCAL);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

//    for (Long l: collection) {
//      logger.debug(Codec.getStringWindowId(l));
//    }
    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
  }

}
