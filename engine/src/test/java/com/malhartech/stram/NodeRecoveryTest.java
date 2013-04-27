/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.CheckpointListener;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.engine.RecoverableInputOperator;
import java.io.IOException;

import java.util.HashSet;
import com.malhartech.netlet.DefaultEventLoop;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NodeRecoveryTest
{
  private static final Logger logger = LoggerFactory.getLogger(NodeRecoveryTest.class);
  static HashSet<Long> collection = new HashSet<Long>(20);

  @Before
  public void setup() throws IOException
  {
    StramChild.eventloop = new DefaultEventLoop("NodeRecoveryTestEventLoop");
  }
  @After
  public void teardown()
  {
    StramChild.eventloop.stop();

  }


  public static class CollectorOperator extends BaseOperator implements CheckpointListener
  {
    private int simulateFailure;
    private transient boolean nexttime;
    public final transient DefaultInputPort<Long> input = new DefaultInputPort<Long>(this)
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
      if (simulateFailure) {
        this.simulateFailure = 1;
      }
      else {
        this.simulateFailure = 0;
      }
    }

    @Override
    public void checkpointed(long windowId)
    {
      if (simulateFailure > 0 && --simulateFailure == 0) {
        nexttime = true;
      }
      else if (nexttime) {
        throw new RuntimeException("Failure Simulation from " + this);
      }
    }

    @Override
    public void committed(long windowId)
    {
    }

  }

  @Test
  public void testInputOperatorRecovery() throws Exception
  {
    collection.clear();
    int maxTuples = 30;
    DAG dag = new DAG();
    dag.getAttributes().attr(DAG.STRAM_CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();

    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

  @Test
  public void testOperatorRecovery() throws Exception
  {
    collection.clear();
    int maxTuples = 30;
    DAG dag = new DAG();
    dag.getAttributes().attr(DAG.STRAM_CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.addStream("connection", rip.output, cm.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
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
    DAG dag = new DAG();
    dag.getAttributes().attr(DAG.STRAM_CHECKPOINT_WINDOW_COUNT).set(2);
    dag.getAttributes().attr(DAG.STRAM_WINDOW_SIZE_MILLIS).set(300);
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(1);
    RecoverableInputOperator rip = dag.addOperator("LongGenerator", RecoverableInputOperator.class);
    rip.setMaximumTuples(maxTuples);

    CollectorOperator cm = dag.addOperator("LongCollector", CollectorOperator.class);
    cm.setSimulateFailure(true);
    dag.addStream("connection", rip.output, cm.input).setInline(true);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run();

//    for (Long l: collection) {
//      logger.debug(Codec.getStringWindowId(l));
//    }
    Assert.assertEquals("Generated Outputs", maxTuples, collection.size());
  }

}
