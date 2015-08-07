/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.CheckpointListener;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 */
public class StreamingContainerTest
{

  @Test
  public void testCommitted() throws IOException, ClassNotFoundException
  {
    LogicalPlan lp = new LogicalPlan();
    String workingDir = new File("target/testCommitted").getAbsolutePath();
    lp.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(workingDir + "/localPath", workingDir, null));
    lp.setAttribute(DAGContext.CHECKPOINT_WINDOW_COUNT, 1);
    CommitAwareOperator operator = lp.addOperator("CommitAwareOperator", new CommitAwareOperator());

    List<Long> myCommittedWindowIds = CommitAwareOperator.getCommittedWindowIdsContainer();

    StramLocalCluster lc = new StramLocalCluster(lp);
    lc.run(5000);

    /* this is not foolproof but some insurance is better than nothing */
    Assert.assertSame("Concurrent Use detected", myCommittedWindowIds, CommitAwareOperator.committedWindowIds);
    Assert.assertFalse("No Committed Windows", myCommittedWindowIds.isEmpty());
  }

  private static class CommitAwareOperator extends BaseOperator implements CheckpointListener, InputOperator
  {
    public static ArrayList<Long> committedWindowIds;
    public static ArrayList<Long> checkpointedWindowIds = new ArrayList<Long>();

    public static synchronized final List<Long> getCommittedWindowIdsContainer()
    {
      return committedWindowIds = new ArrayList<Long>();
    }

    @Override
    public void checkpointed(long windowId)
    {
      checkpointedWindowIds.add(windowId);
      logger.debug("checkpointed {}", windowId);
    }

    @Override
    public void committed(long windowId)
    {
      committedWindowIds.add(windowId);
      logger.debug("committed {}", windowId);
    }

    @Override
    public void emitTuples()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(CommitAwareOperator.class);
  }

}
