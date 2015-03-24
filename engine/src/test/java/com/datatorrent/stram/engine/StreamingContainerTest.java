package com.datatorrent.stram.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.CheckpointListener;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 * @author Chetan Narsude  <chetan@datatorrent.com>
 */
public class StreamingContainerTest
{

  @Test
  public void testCommitted() throws IOException, ClassNotFoundException
  {
    LogicalPlan lp = new LogicalPlan();
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
