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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG.Locality;

import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.ProcessingModeTests.CollectorOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
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
    String workingDir = new File("target/testInputOperatorRecovery").getAbsolutePath();
    AsyncFSStorageAgent asyncFSStorageAgent = new AsyncFSStorageAgent(workingDir, null);
    asyncFSStorageAgent.setSyncCheckpoint(true);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, asyncFSStorageAgent);
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
    String workingDir = new File("target/testOperatorRecovery").getAbsolutePath();
    AsyncFSStorageAgent asyncFSStorageAgent = new AsyncFSStorageAgent(workingDir, null);
    asyncFSStorageAgent.setSyncCheckpoint(true);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, asyncFSStorageAgent);
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
    String workingDir = new File("target/testOperatorRecovery").getAbsolutePath();
    AsyncFSStorageAgent asyncFSStorageAgent = new AsyncFSStorageAgent(workingDir, null);
    asyncFSStorageAgent.setSyncCheckpoint(true);
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, asyncFSStorageAgent);
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
