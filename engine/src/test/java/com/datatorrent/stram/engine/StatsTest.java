/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StatsListener.BatchedOperatorStats;
import com.datatorrent.api.StatsListener.Response;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.StatsTest.TestOperator.TestInputStatsListener;
import com.datatorrent.stram.engine.StatsTest.TestOperator.TestOutputStatsListener;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;

/**
 * Tests the stats generated in the system.
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class StatsTest
{
  private static final Logger LOG = LoggerFactory.getLogger(StatsTest.class);

  public static class TestOperator extends TestGeneratorInputOperator
  {
    private static List<OperatorStats> inputOperatorStats = new ArrayList<OperatorStats>();
    private static List<OperatorStats> outputOperatorStats = new ArrayList<OperatorStats>();

    public static class TestInputStatsListener implements StatsListener
    {
      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        inputOperatorStats.addAll(stats.getLastWindowedStats());
        Response rsp = new Response();
        return rsp;
      }

    }

    public static class TestOutputStatsListener implements StatsListener
    {
      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        outputOperatorStats.addAll(stats.getLastWindowedStats());
        Response rsp = new Response();
        return rsp;
      }

    }

  }

  /**
   * Verify buffer server bytes and tuple count.
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testPortStatsPropagation() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);

    TestOperator testOper = dag.addOperator("TestOperator", TestOperator.class);
    dag.setAttribute(testOper, OperatorContext.STATS_LISTENER, TestOutputStatsListener.class);

    GenericTestOperator collector = dag.addOperator("Collector", new GenericTestOperator());
    dag.setAttribute(collector, OperatorContext.STATS_LISTENER, TestInputStatsListener.class);
    dag.addStream("TestTuples", testOper.outport, collector.inport1).setLocality(null);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();

    long startTms = System.currentTimeMillis();
    while (TestOperator.inputOperatorStats.isEmpty() && TestOperator.outputOperatorStats.isEmpty() && StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      Thread.sleep(300);
      LOG.debug("Waiting for stats");
    }

    try {
      int inputPortTupleCount = 0;
      int outputPortTupleCount = 0;

      for (OperatorStats operatorStats : TestOperator.inputOperatorStats) {
        for (PortStats inputPortStats : operatorStats.inputPorts) {
          if (inputPortStats.tupleCount > 0) {
            inputPortTupleCount += inputPortStats.tupleCount;
            Assert.assertTrue("Validate input port buffer server bytes", inputPortStats.bufferServerBytes > 0);
          }
          else {
            Assert.assertTrue("Validate input port buffer server bytes", inputPortStats.bufferServerBytes == 0);
          }
        }
      }

      for (OperatorStats operatorStats : TestOperator.outputOperatorStats) {
        for (PortStats outputPortStats : operatorStats.outputPorts) {
          if (outputPortStats.tupleCount > 0) {
            outputPortTupleCount += outputPortStats.tupleCount;
            Assert.assertTrue("Validate output port buffer server bytes", outputPortStats.bufferServerBytes > 0);
          }
          else {
            Assert.assertTrue("Validate output port buffer server bytes", outputPortStats.bufferServerBytes == 0);
          }
        }
      }

      Assert.assertTrue("Tuple Count emitted and processed", inputPortTupleCount == outputPortTupleCount);
    }
    finally {
      lc.shutdown();
    }
  }

}
