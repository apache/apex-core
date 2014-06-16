/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
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
    StreamingContainer.eventloop.start();
  }

  @After
  public void teardown()
  {
    StreamingContainer.eventloop.stop();

  }

  @Test
  public void testInputOperatorRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
  }

  @Test
  public void testOperatorRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, CollectorOperator.collection.size());
  }

  @Test
  public void testInlineOperatorsRecovery() throws Exception
  {
    RecoverableInputOperator.initGenTuples();
    CollectorOperator.collection.clear();
    int maxTuples = 30;
    LogicalPlan dag = new LogicalPlan();
    //dag.getAttributes().get(DAG.HEARTBEAT_INTERVAL_MILLIS, 400);
    dag.getAttributes().put(LogicalPlan.CHECKPOINT_WINDOW_COUNT, 2);
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);
    rip.setSimulateFailure(true);

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
