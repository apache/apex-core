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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StramLocalCluster.LocalStreamingContainer;
import com.datatorrent.stram.StreamingContainerAgent;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.StramTestSupport;

/**
 *
 */
public class StreamingContainerTest
{
  private static final Logger logger =  LoggerFactory.getLogger(StreamingContainerTest.class);
  private static Map<String, Integer> activationCounts = Collections.synchronizedMap(new HashMap<String, Integer>());
  private static Set<String> committedWindowIds = Collections.synchronizedSet(new HashSet<String>());
  private static Set<String> checkpointedWindowIds = Collections.synchronizedSet(new HashSet<String>());

  @Before
  public void initialize()
  {
    activationCounts.clear();
    committedWindowIds.clear();
    checkpointedWindowIds.clear();
  }

  @Test
  public void testCommitted() throws IOException, ClassNotFoundException
  {
    LogicalPlan lp = new LogicalPlan();
    String workingDir = new File("target/testCommitted").getAbsolutePath();
    lp.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir, null));
    lp.setAttribute(DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    String opName = "CommitAwareOperatorTestCommit";
    lp.addOperator(opName, new CommitAwareOperator());

    StramLocalCluster lc = new StramLocalCluster(lp);
    lc.run(5000);

    /* this is not foolproof but some insurance is better than nothing */
    Assert.assertTrue("No Committed Windows", committedWindowIds.contains(opName));
  }

  @Test
  public void testOiOCommitted() throws IOException, ClassNotFoundException
  {
    LogicalPlan lp = new LogicalPlan();
    String workingDir = new File("target/testCommitted").getAbsolutePath();
    lp.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir, null));
    lp.setAttribute(DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    String op1Name = "CommitAwareOperatorTestOioCommit1";
    String op2Name = "CommitAwareOperatorTestOioCommit2";
    CommitAwareOperator operator1 = lp.addOperator(op1Name, new CommitAwareOperator());
    CommitAwareOperator operator2 = lp.addOperator(op2Name, new CommitAwareOperator());
    lp.addStream("local", operator1.output, operator2.input).setLocality(Locality.THREAD_LOCAL);

    StramLocalCluster lc = new StramLocalCluster(lp);
    lc.run(5000);

    /* this is not foolproof but some insurance is better than nothing */
    Assert.assertTrue("No Committed Windows", committedWindowIds.contains(op1Name));
    Assert.assertTrue("No Committed Windows", committedWindowIds.contains(op2Name));
  }

  @SuppressWarnings("rawtypes")
  private static class CommitAwareOperator extends BaseOperator implements CheckpointListener, InputOperator, Operator.ActivationListener
  {
    private transient String name;
    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

    @InputPortFieldAnnotation(optional = true)
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
      }
    };
    @Override
    public void setup(OperatorContext context)
    {
      this.name = context.getName();
    }

    @Override
    public void checkpointed(long windowId)
    {
      checkpointedWindowIds.add(name);
      logger.debug("checkpointed {} {}", name, windowId);
    }

    @Override
    public void committed(long windowId)
    {
      committedWindowIds.add(name);
      logger.debug("committed {} {}", name, windowId);
    }

    @Override
    public void emitTuples()
    {
    }

    @Override
    public void activate(Context context)
    {
      logger.debug("activate called for {}", name);
      Integer val = activationCounts.get(name);
      if (val == null) {
        val = new Integer(0);
      }
      activationCounts.put(name, ++val);
    }

    @Override
    public void deactivate()
    {
    }
  }

  @Test
  public void testActivateOioRedeployed() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    String workingDir = new File("target/testCommitted").getAbsolutePath();
    String upstreamOpName = "UpstreamOperator";
    String op1Name = "CommitAwareOperatorTestOioCommit1";
    String op2Name = "CommitAwareOperatorTestOioCommit2";
    CommitAwareOperator upstream = dag.addOperator(upstreamOpName, new CommitAwareOperator());
    CommitAwareOperator node1 = dag.addOperator(op1Name, new CommitAwareOperator());
    CommitAwareOperator node2 = dag.addOperator(op2Name, new CommitAwareOperator());
    dag.addStream("upstream", upstream.output, node1.input);
    dag.addStream("local", node1.output, node2.input).setLocality(Locality.THREAD_LOCAL);

    dag.setAttribute(OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir, null));
    dag.validate();

    StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.runAsync();

    PTOperator ptUpstream = localCluster.findByLogicalNode(dag.getMeta(upstream));
    PTOperator ptNode2 = localCluster.findByLogicalNode(dag.getMeta(node2));

    LocalStreamingContainer c0 = StramTestSupport.waitForActivation(localCluster, ptUpstream);
    LocalStreamingContainer c2 = StramTestSupport.waitForActivation(localCluster, ptNode2);

    // fail the upstream operator to re-deploy downstream operators
    activationCounts.clear();
    localCluster.failContainer(c0);

    // operators will deploy after downstream operator was removed
    LocalStreamingContainer c0Replaced = StramTestSupport.waitForActivation(localCluster, ptUpstream);
    c0Replaced.triggerHeartbeat();
    c0Replaced.waitForHeartbeat(5000); // next heartbeat after setup

    // wait for downstream re-deploy to complete
    StreamingContainerAgent c2Agent = localCluster.getContainerAgent(c2);
    long startTms = System.currentTimeMillis();
    while (c2Agent.hasPendingWork() && StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      Thread.sleep(200);
      c2.triggerHeartbeat();
      logger.debug("Waiting for {} to complete pending work.", c2.getContainerId());
    }
    Assert.assertEquals("Expected activation counts", 1, activationCounts.get(upstreamOpName).intValue());
    Assert.assertEquals("Expected activation counts", 1, activationCounts.get(op1Name).intValue());
    Assert.assertEquals("Expected activation counts", 1, activationCounts.get(op2Name).intValue());
    localCluster.shutdown();
  }

}
