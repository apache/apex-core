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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.NotNull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StatsListener;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.AutoMetricTest.TestOperator.TestStatsListener;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;
import com.datatorrent.stram.support.StramTestSupport;

public class AutoMetricTest
{
  private static final Logger LOG = LoggerFactory.getLogger(AutoMetricTest.class);

  public static class TestOperator extends TestGeneratorInputOperator implements Partitioner<TestOperator>, StatsListener
  {
    static class TestOperatorStats implements Serializable
    {
      private String message;
      private boolean attributeListenerCalled;
      private static final long serialVersionUID = -8096838101190642798L;
      private boolean currentPropVal;
    }

    public static class TestStatsListener implements StatsListener, Serializable
    {
      private static final long serialVersionUID = 1L;
      private boolean lastPropVal;

      @Override
      public Response processStats(BatchedOperatorStats stats)
      {
        for (OperatorStats os : stats.getLastWindowedStats()) {
          Assert.assertNotNull("metrics", os.metrics.get("operatorMetric"));
          TestOperatorStats loperatorStats = (TestOperatorStats)os.metrics.get("operatorMetric");
          loperatorStats.attributeListenerCalled = true;
          lastPropVal = loperatorStats.currentPropVal;
        }
        if (lastPropVal) {
          Assert.assertNotNull(stats.getOperatorResponse());
          Assert.assertTrue(1 == stats.getOperatorResponse().size());
          Assert.assertEquals("test", stats.getOperatorResponse().get(0).getResponse());
        }
        Response rsp = new Response();
        rsp.operatorRequests = Lists.newArrayList(new SetPropertyRequest());
        return rsp;
      }

      public static class SetPropertyRequest implements OperatorRequest, Serializable
      {
        private static final long serialVersionUID = 1L;
        @Override
        public OperatorResponse execute(Operator oper, int arg1, long arg2) throws IOException
        {
          if (oper instanceof TestOperator) {
            LOG.debug("Setting property");
            ((TestOperator)oper).propVal = true;
          }
          return new TestOperatorResponse();
        }
      }

      public static class TestOperatorResponse implements OperatorResponse, Serializable
      {
        private static final long serialVersionUID = 2L;

        @Override
        public Object getResponseId()
        {
          return 1;
        }

        @Override
        public Object getResponse()
        {
          return "test";
        }
      }
    }

    private static TestOperatorStats lastMetric = null;
    private static Thread processStatsThread = null;
    private static Thread definePartitionsThread = null;
    private transient boolean propVal;

    @AutoMetric
    private TestOperatorStats operatorMetric;

    @Override
    public void partitioned(Map<Integer, Partition<TestOperator>> partitions)
    {
    }

    @Override
    public Collection<Partition<TestOperator>> definePartitions(Collection<Partition<TestOperator>> partitions, PartitioningContext context)
    {
      List<Partition<TestOperator>> newPartitions = Lists.newArrayList();
      newPartitions.addAll(partitions);

      for (Partition<?> p : partitions) {
        BatchedOperatorStats stats = p.getStats();
        if (stats != null) {
          definePartitionsThread = Thread.currentThread();
          for (OperatorStats os : stats.getLastWindowedStats()) {
            if (os.metrics.get("operatorMetric") != null) {
              lastMetric = (TestOperatorStats)os.metrics.get("operatorMetric");
            }
          }
        }
      }
      return newPartitions;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      operatorMetric = new TestOperatorStats();
      operatorMetric.message = "interesting";
      operatorMetric.currentPropVal = this.propVal;
    }

    @Override
    public Response processStats(BatchedOperatorStats stats)
    {
      processStatsThread = Thread.currentThread();
      for (OperatorStats os : stats.getLastWindowedStats()) {
        Assert.assertNotNull("metric in listener", os.metrics.get("operatorMetric"));
      }
      Response rsp = new Response();
      rsp.repartitionRequired = true; // trigger definePartitions
      return rsp;
    }

  }

  private LogicalPlan dag;

  @Before
  public void setup()
  {
    dag = StramTestSupport.createDAG(testMeta);
  }

  /**
   * Verify custom stats generated by operator are propagated and trigger repartition.
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testMetricPropagation() throws Exception
  {
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 300);
    dag.getAttributes().put(LogicalPlan.CONTAINERS_MAX_COUNT, 1);

    TestOperator testOper = dag.addOperator("TestOperator", TestOperator.class);
    TestStatsListener sl = new TestStatsListener();
    dag.setOperatorAttribute(testOper, OperatorContext.STATS_LISTENERS, Lists.newArrayList((StatsListener)sl));

    GenericTestOperator collector = dag.addOperator("Collector", new GenericTestOperator());
    dag.addStream("TestTuples", testOper.outport, collector.inport1).setLocality(Locality.CONTAINER_LOCAL);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();

    long startTms = System.currentTimeMillis();
    while (TestOperator.lastMetric == null && StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      Thread.sleep(300);
      LOG.debug("Waiting for stats");
    }

    while (StramTestSupport.DEFAULT_TIMEOUT_MILLIS > System.currentTimeMillis() - startTms) {
      if (sl.lastPropVal) {
        break;
      }
      Thread.sleep(100);
      LOG.debug("Waiting for property set");
    }

    lc.shutdown();

    Assert.assertNotNull("metric received", TestOperator.lastMetric);
    Assert.assertEquals("metric message", "interesting", TestOperator.lastMetric.message);
    Assert.assertTrue("attribute defined stats listener called", TestOperator.lastMetric.attributeListenerCalled);
    Assert.assertSame("single thread", TestOperator.definePartitionsThread, TestOperator.processStatsThread);
    Assert.assertTrue("property set", sl.lastPropVal);
  }

  @Rule
  public StramTestSupport.TestMeta testMeta = new StramTestSupport.TestMeta();

  @Test
  public void testMetrics() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    TestGeneratorInputOperator inputOperator = dag.addOperator("input", TestGeneratorInputOperator.class);

    OperatorWithMetrics o1 = dag.addOperator("o1", OperatorWithMetrics.class);
    MockAggregator aggregator = new MockAggregator(latch);
    dag.setOperatorAttribute(o1, Context.OperatorContext.METRICS_AGGREGATOR, aggregator);

    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    dag.addStream("TestTuples", inputOperator.outport, o1.inport1);

    lpc.prepareDAG(dag, null, "AutoMetricTest");

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();
    latch.await();

    Assert.assertEquals("progress", 1L, ((Long)aggregator.result.get("progress")).longValue());
    lc.shutdown();
  }

  @Test
  @Ignore
  public void testMetricsAggregations() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(2);

    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    TestGeneratorInputOperator inputOperator = dag.addOperator("input", TestGeneratorInputOperator.class);

    OperatorWithMetrics o1 = dag.addOperator("o1", OperatorWithMetrics.class);
    MockAggregator aggregator = new MockAggregator(latch);
    dag.setOperatorAttribute(o1, Context.OperatorContext.METRICS_AGGREGATOR, aggregator);

    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());
    dag.setOperatorAttribute(o1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TestGeneratorInputOperator>(2));

    dag.addStream("TestTuples", inputOperator.outport, o1.inport1);

    lpc.prepareDAG(dag, null, "AutoMetricTest");
    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();
    latch.await();
    Assert.assertEquals("progress", 2L, ((Long)aggregator.result.get("progress")).longValue());
    lc.shutdown();
  }

  @Test
  public void testInjectionOfDefaultMetricsAggregator() throws Exception
  {
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    TestGeneratorInputOperator inputOperator = dag.addOperator("input", TestGeneratorInputOperator.class);

    OperatorWithMetricMethod o1 = dag.addOperator("o1", OperatorWithMetricMethod.class);

    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    dag.addStream("TestTuples", inputOperator.outport, o1.inport1);

    lpc.prepareDAG(dag, null, "AutoMetricTest");

    LogicalPlan.OperatorMeta o1meta = dag.getOperatorMeta("o1");
    Assert.assertNotNull("default aggregator injected", o1meta.getMetricAggregatorMeta().getAggregator());
  }

  @Test
  public void testDefaultMetricsAggregator() throws Exception
  {
    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    TestGeneratorInputOperator inputOperator = dag.addOperator("input", TestGeneratorInputOperator.class);

    CountDownLatch latch = new CountDownLatch(1);
    OperatorAndAggregator o1 = dag.addOperator("o1", new OperatorAndAggregator(latch));

    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    dag.addStream("TestTuples", inputOperator.outport, o1.inport1);

    lpc.prepareDAG(dag, null, "AutoMetricTest");

    LogicalPlan.OperatorMeta o1meta = dag.getOperatorMeta("o1");
    Assert.assertNotNull("default aggregator injected", o1meta.getMetricAggregatorMeta().getAggregator());

    lpc.prepareDAG(dag, null, "AutoMetricTest");
    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();
    latch.await();
    Assert.assertEquals("progress", 1, o1.result.get("progress"));
    lc.shutdown();
  }

  private static class MockAggregator implements AutoMetric.Aggregator, Serializable
  {
    long cachedSum = -1;
    Map<String, Object> result = Maps.newHashMap();
    transient CountDownLatch latch;

    private MockAggregator(CountDownLatch latch)
    {
      this.latch = Preconditions.checkNotNull(latch);
    }

    @Override
    public Map<String, Object> aggregate(long windowId, Collection<AutoMetric.PhysicalMetricsContext> physicalMetrics)
    {
      long sum = 0;
      int myMetricSum = 0;
      for (AutoMetric.PhysicalMetricsContext physicalMetricsContext : physicalMetrics) {
        sum += (Integer)physicalMetricsContext.getMetrics().get("progress");
        if (physicalMetricsContext.getMetrics().containsKey("myMetric")) {
          myMetricSum += (Integer)physicalMetricsContext.getMetrics().get("myMetric");
        }
      }
      cachedSum = sum;
      result.put("progress", cachedSum);
      result.put("myMetric", myMetricSum);
      latch.countDown();
      return result;
    }

    private static final long serialVersionUID = 201503311744L;
  }

  public static class OperatorWithMetrics extends GenericTestOperator
  {
    @AutoMetric
    protected int progress;

    @Override
    public void endWindow()
    {
      progress = 1;
      super.endWindow();
    }
  }

  public static class OperatorWithMetricMethod extends OperatorWithMetrics
  {
    @AutoMetric
    public int getMyMetric()
    {
      return 3;
    }
  }

  public static class OperatorAndAggregator extends OperatorWithMetrics implements AutoMetric.Aggregator
  {
    Map<String, Object> result = Maps.newHashMap();

    private final transient CountDownLatch latch;

    private OperatorAndAggregator()
    {
      latch = null;
    }

    OperatorAndAggregator(@NotNull CountDownLatch latch)
    {
      this.latch = Preconditions.checkNotNull(latch);
    }

    @Override
    public Map<String, Object> aggregate(long windowId, Collection<AutoMetric.PhysicalMetricsContext> physicalMetrics)
    {
      result.put("progress", physicalMetrics.iterator().next().getMetrics().get("progress"));
      latch.countDown();
      return result;
    }
  }

  @Test
  public void testMetricsAnnotatedMethod() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(1);

    LogicalPlanConfiguration lpc = new LogicalPlanConfiguration(new Configuration());

    TestGeneratorInputOperator inputOperator = dag.addOperator("input", TestGeneratorInputOperator.class);

    OperatorWithMetricMethod o1 = dag.addOperator("o1", OperatorWithMetricMethod.class);
    MockAggregator aggregator = new MockAggregator(latch);
    dag.setOperatorAttribute(o1, Context.OperatorContext.METRICS_AGGREGATOR, aggregator);

    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new StramTestSupport.MemoryStorageAgent());

    dag.addStream("TestTuples", inputOperator.outport, o1.inport1);

    lpc.prepareDAG(dag, null, "AutoMetricTest");

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.runAsync();
    latch.await();

    Assert.assertEquals("myMetric", 3, ((Integer)aggregator.result.get("myMetric")).intValue());
    lc.shutdown();
  }
}
