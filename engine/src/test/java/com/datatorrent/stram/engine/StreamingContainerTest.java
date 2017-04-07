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
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
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
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 */
public class StreamingContainerTest
{
  private static final Logger logger =  LoggerFactory.getLogger(StreamingContainerTest.class);
  private static Set<String> committedWindowIds = Collections.synchronizedSet(new HashSet<String>());
  private static Set<String> checkpointedWindowIds = Collections.synchronizedSet(new HashSet<String>());

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

  private static class CommitAwareOperator extends BaseOperator implements CheckpointListener, InputOperator
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

  }

}
