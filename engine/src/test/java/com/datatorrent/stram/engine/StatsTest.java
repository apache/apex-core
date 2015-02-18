/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.Stats.OperatorStats.PortStats;
import com.datatorrent.api.StatsListener;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.StatsTest.TestCollector.TestCollectorStatsListener;
import com.datatorrent.stram.engine.StatsTest.TestOperator.TestInputStatsListener;
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
    transient long windowId;
    boolean shutdown = false; // make sure shutdown happens after endwindow when stats are generated

    @Override
    public void beginWindow(long windowId)
    {
      if (shutdown) {
        BaseOperator.shutdown();
      }
      this.windowId = windowId;
    }

    @Override
    public void emitTuples()
    {
      try {
        if (!shutdown) {
          super.emitTuples();
        }
      }
      catch (ShutdownException ex) {
        shutdown = true;
      }
    }

    public static class TestInputStatsListener implements StatsListener, Serializable
    {
      private static final long serialVersionUID = 1L;
      private List<OperatorStats> inputOperatorStats = new ArrayList<OperatorStats>();

      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        inputOperatorStats.addAll(stats.getLastWindowedStats());
        Response rsp = new Response();
        return rsp;
      }

    }

  }

  public static class TestCollector extends GenericTestOperator
  {
    transient long windowId;

    @Override
    public void beginWindow(long windowId)
    {
      this.windowId = windowId;
    }

    public static class TestCollectorStatsListener implements StatsListener, Serializable
    {
      private static final long serialVersionUID = 1L;
      List<OperatorStats> collectorOperatorStats = new ArrayList<OperatorStats>();

      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        collectorOperatorStats.addAll(stats.getLastWindowedStats());
        Response rsp = new Response();
        return rsp;
      }

      public void validateStats()
      {
        for (OperatorStats operatorStats : collectorOperatorStats) {
          for (PortStats inputPortStats : operatorStats.inputPorts) {
            Assert.assertTrue("Validate input port queue size " + inputPortStats.queueSize, inputPortStats.queueSize >= 0);
          }
        }
      }
    }

    @StatsListener.QUEUE_SIZE_AWARE
    public static class QueueAwareTestCollectorStatsListener extends TestCollectorStatsListener
    {
      private static final long serialVersionUID = 2L;

      public void validateStats()
      {
        for (OperatorStats operatorStats : collectorOperatorStats) {
          for (PortStats inputPortStats : operatorStats.inputPorts) {
            Assert.assertTrue("Validate input port queue size " + inputPortStats.queueSize, inputPortStats.queueSize == 0);
          }
        }
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
    int tupleCount = 10;
    LogicalPlan dag = new LogicalPlan();

    TestOperator testOper = dag.addOperator("TestOperator", TestOperator.class);
    TestInputStatsListener testInputStatsListener = new TestInputStatsListener();
    dag.setAttribute(testOper, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{testInputStatsListener}));
    testOper.setMaxTuples(tupleCount);
    testOper.setEmitInterval(0);

    TestCollector collector = dag.addOperator("Collector", new TestCollector());
    TestCollectorStatsListener testCollectorStatsListener = new TestCollectorStatsListener();
    dag.setAttribute(collector, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{testCollectorStatsListener}));
    dag.addStream("TestTuples", testOper.outport, collector.inport1).setLocality(null);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run();

    Assert.assertFalse("input operator stats", testInputStatsListener.inputOperatorStats.isEmpty());
    Assert.assertFalse("collector operator stats", testCollectorStatsListener.collectorOperatorStats.isEmpty());
    try {
      int outputPortTupleCount = 0;
      long outputPortBufferServerBytes = 0L;

      for (Iterator<OperatorStats> it = testInputStatsListener.inputOperatorStats.iterator(); it.hasNext(); ) {
        OperatorStats operatorStats = it.next();
        for (PortStats outputPortStats : operatorStats.outputPorts) {
          outputPortTupleCount += outputPortStats.tupleCount;
          outputPortBufferServerBytes += outputPortStats.bufferServerBytes;
        }
      }

      int inputPortTupleCount = 0;
      long inputPortBufferServerBytes = 0L;

      for (Iterator<OperatorStats> it = testCollectorStatsListener.collectorOperatorStats.iterator(); it.hasNext(); ) {
        OperatorStats operatorStats = it.next();
        for (PortStats inputPortStats : operatorStats.inputPorts) {
          inputPortTupleCount += inputPortStats.tupleCount;
          inputPortBufferServerBytes += inputPortStats.bufferServerBytes;
        }
      }

      Assert.assertEquals("Tuple Count emitted", tupleCount, outputPortTupleCount);
      Assert.assertTrue("Buffer server bytes", inputPortBufferServerBytes > 0);

      Assert.assertEquals("Tuple Count processed", tupleCount, inputPortTupleCount);
      Assert.assertTrue("Buffer server bytes", outputPortBufferServerBytes > 0);
    }
    finally {
      lc.shutdown();
    }
  }

  @SuppressWarnings("SleepWhileInLoop")
  private void baseTestForQueueSize(int maxTuples, TestCollectorStatsListener statsListener, DAG.Locality locality) throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    TestOperator testOper = dag.addOperator("TestOperator", TestOperator.class);
    testOper.setMaxTuples(maxTuples);

    TestCollector collector = dag.addOperator("Collector", new TestCollector());
    dag.setAttribute(collector, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{statsListener}));
    dag.addStream("TestTuples", testOper.outport, collector.inport1).setLocality(locality);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();

    long startTms = System.currentTimeMillis();
    while ((statsListener.collectorOperatorStats.isEmpty() || testOper.windowId > collector.windowId) && StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      Thread.sleep(300);
      LOG.debug("Waiting for stats");
    }
    statsListener.validateStats();
    lc.shutdown();
  }

  /**
   * Verify queue size.
   *
   * @throws Exception
   */
  @Test
  public void testQueueSizeForContainerLocalOperators() throws Exception
  {
    baseTestForQueueSize(10, new TestCollectorStatsListener(), DAG.Locality.CONTAINER_LOCAL);
  }

  @Test
  public void testQueueSize() throws Exception
  {
    baseTestForQueueSize(10, new TestCollectorStatsListener(), null);
  }


  /**
   * Verify queue size.
   *
   * @throws Exception
   */
  @Test
  public void testQueueSizeWithQueueAwareStatsListenerForContainerLocalOperators() throws Exception
  {
    baseTestForQueueSize(0, new TestCollector.QueueAwareTestCollectorStatsListener(), DAG.Locality.CONTAINER_LOCAL);
  }

  @Test
  public void testQueueSizeWithQueueAwareStatsListener() throws Exception
  {
    baseTestForQueueSize(0, new TestCollector.QueueAwareTestCollectorStatsListener(), null);
  }

}
