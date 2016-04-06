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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.ScheduledExecutorService;
import com.datatorrent.common.util.ScheduledThreadPoolExecutor;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 *
 */
public class GenericNodeTest
{
  @Rule
  public FSTestWatcher testMeta = new FSTestWatcher();

  public static class FSTestWatcher extends TestWatcher
  {
    private String dir;

    public String getDir()
    {
      return dir;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      dir = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(org.junit.runner.Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(dir));
    }
  }

  public static class DelayAsyncFSStorageAgent extends AsyncFSStorageAgent
  {
    private static final long serialVersionUID = 201511301205L;

    public DelayAsyncFSStorageAgent(String localBasePath, String path, Configuration conf)
    {
      super(localBasePath, path, conf);
    }

    private long delayMS = 2000L;

    public DelayAsyncFSStorageAgent(String path, Configuration conf)
    {
      super(path, conf);
    }

    @Override
    public void save(final Object object, final int operatorId, final long windowId) throws IOException
    {
      //Do nothing
    }

    @Override
    public void copyToHDFS(int operatorId, long windowId) throws IOException
    {
      try {
        Thread.sleep(delayMS);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    /**
     * @return the delayMS
     */
    public long getDelayMS()
    {
      return delayMS;
    }

    /**
     * @param delayMS the delayMS to set
     */
    public void setDelayMS(long delayMS)
    {
      this.delayMS = delayMS;
    }
  }

  public static class TestStatsOperatorContext extends OperatorContext
  {
    private static final long serialVersionUID = 201511301206L;

    public volatile List<Checkpoint> checkpoints = Lists.newArrayList();

    public TestStatsOperatorContext(int id, AttributeMap attributes, Context parentContext)
    {
      super(id, attributes, parentContext);
    }

    @Override
    public void report(OperatorStats stats, long windowId)
    {
      super.report(stats, windowId);

      if (stats.checkpoint != null) {
        checkpoints.add((Checkpoint)stats.checkpoint);
      }
    }
  }

  public static class GenericOperator implements Operator
  {
    Context.OperatorContext context;
    long beginWindowId;
    long endWindowId;
    public final transient DefaultInputPort<Object> ip1 = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        op.emit(tuple);
      }

    };
    @InputPortFieldAnnotation( optional = true)
    public final transient DefaultInputPort<Object> ip2 = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        op.emit(tuple);
      }

    };
    @OutputPortFieldAnnotation( optional = true)
    DefaultOutputPort<Object> op = new DefaultOutputPort<Object>();

    @Override
    public void beginWindow(long windowId)
    {
      beginWindowId = windowId;
    }

    @Override
    public void endWindow()
    {
      endWindowId = beginWindowId;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
      this.context = context;
    }

    @Override
    public void teardown()
    {
    }

  }

  public static class CheckpointDistanceOperator extends GenericOperator
  {
    List<Integer> distances = new ArrayList<Integer>();
    int numWindows = 0;
    int maxWindows = 0;

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
      if (numWindows++ < maxWindows) {
        distances.add(context.getWindowsFromCheckpoint());
      }
    }
  }

  public static class GenericCheckpointOperator extends GenericOperator implements CheckpointNotificationListener
  {
    public Set<Long> checkpointedWindows = Sets.newHashSet();
    public volatile boolean checkpointTwice = false;
    public volatile int numWindows = 0;

    public GenericCheckpointOperator()
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
      super.beginWindow(windowId);
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      numWindows++;
    }

    @Override
    public void checkpointed(long windowId)
    {
      checkpointTwice = checkpointTwice || !checkpointedWindows.add(windowId);
    }

    @Override
    public void committed(long windowId)
    {
    }

    @Override
    public void beforeCheckpoint(long windowId)
    {
    }
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testSynchingLogic() throws InterruptedException
  {
    long sleeptime = 25L;
    final ArrayList<Object> list = new ArrayList<Object>();
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, new DefaultAttributeMap(), null));
    gn.setId(1);
    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("ip1Res", 1024);
    AbstractReservoir reservoir2 = AbstractReservoir.newReservoir("ip2Res", 1024);
    Sink<Object> output = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        list.add(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };

    gn.connectInputPort("ip1", reservoir1);
    gn.connectInputPort("ip2", reservoir2);
    gn.connectOutputPort("op", output);
    gn.firstWindowMillis = 0;
    gn.windowWidthMillis = 100;

    final AtomicBoolean ab = new AtomicBoolean(false);
    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        ab.set(true);
        gn.activate();
        gn.run();
        gn.deactivate();
      }

    };
    t.start();

    do {
      Thread.sleep(sleeptime);
    } while (ab.get() == false);


    Tuple beginWindow1 = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);

    reservoir1.add(beginWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    reservoir2.add(beginWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    Tuple endWindow1 = new EndWindowTuple(0x1L);

    reservoir1.add(endWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    Tuple beginWindow2 = new Tuple(MessageType.BEGIN_WINDOW, 0x2L);

    reservoir1.add(beginWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(1, list.size());

    reservoir2.add(endWindow1);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    reservoir2.add(beginWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    Tuple endWindow2 = new EndWindowTuple(0x2L);

    reservoir2.add(endWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(3, list.size());

    reservoir1.add(endWindow2);
    Thread.sleep(sleeptime);
    Assert.assertEquals(4, list.size());

    EndStreamTuple est = new EndStreamTuple(0L);

    reservoir1.add(est);
    Thread.sleep(sleeptime);
    Assert.assertEquals(4, list.size());

    Tuple beginWindow3 = new Tuple(MessageType.BEGIN_WINDOW, 0x3L);

    reservoir2.add(beginWindow3);
    Thread.sleep(sleeptime);
    Assert.assertEquals(5, list.size());

    Tuple endWindow3 = new EndWindowTuple(0x3L);

    reservoir2.add(endWindow3);
    Thread.sleep(sleeptime);
    Assert.assertEquals(6, list.size());

    Assert.assertNotSame(Thread.State.TERMINATED, t.getState());

    reservoir2.add(est);
    Thread.sleep(sleeptime);
    Assert.assertEquals(7, list.size());

    Thread.sleep(sleeptime);

    Assert.assertEquals(Thread.State.TERMINATED, t.getState());
  }

  @Test
  public void testPrematureTermination() throws InterruptedException
  {
    long maxSleep = 5000;
    long sleeptime = 25L;
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, new DefaultAttributeMap(), null));
    gn.setId(1);
    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("ip1Res", 1024);
    AbstractReservoir reservoir2 = AbstractReservoir.newReservoir("ip2Res", 1024);

    gn.connectInputPort("ip1", reservoir1);
    gn.connectInputPort("ip2", reservoir2);
    gn.connectOutputPort("op", Sink.BLACKHOLE);
    gn.firstWindowMillis = 0;
    gn.windowWidthMillis = 100;

    final AtomicBoolean ab = new AtomicBoolean(false);
    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        ab.set(true);
        gn.activate();
        gn.run();
        gn.deactivate();
      }

    };
    t.start();

    long interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((ab.get() == false) && (interval < maxSleep));


    int controlTupleCount = gn.controlTupleCount;
    Tuple beginWindow1 = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);

    reservoir1.add(beginWindow1);
    reservoir2.add(beginWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("Begin window called", go.endWindowId != go.beginWindowId);
    controlTupleCount = gn.controlTupleCount;

    Tuple endWindow1 = new EndWindowTuple(0x1L);

    reservoir1.add(endWindow1);
    reservoir2.add(endWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("End window called", go.endWindowId == go.beginWindowId);
    controlTupleCount = gn.controlTupleCount;

    Tuple beginWindow2 = new Tuple(MessageType.BEGIN_WINDOW, 0x2L);

    reservoir1.add(beginWindow2);
    reservoir2.add(beginWindow2);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));

    gn.shutdown();
    t.join();

    Assert.assertTrue("End window not called", go.endWindowId != go.beginWindowId);
  }

  @Test
  public void testDoubleCheckpointAtleastOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.AT_LEAST_ONCE, true, testMeta.getDir());
  }

  @Test
  public void testDoubleCheckpointAtMostOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.AT_MOST_ONCE, true, testMeta.getDir());
  }

  @Test
  public void testDoubleCheckpointExactlyOnce() throws Exception
  {
    NodeTest.testDoubleCheckpointHandling(ProcessingMode.EXACTLY_ONCE, true, testMeta.getDir());
  }

  /**
   * This tests to make sure that the race condition reported in APEX-83 is fixed.
   */
  @Test
  public void testCheckpointApplicationWindowCountAtleastOnce() throws Exception
  {
    testCheckpointApplicationWindowCount(ProcessingMode.AT_LEAST_ONCE);
  }

  /**
   * This tests to make sure that the race condition reported in APEX-83 is fixed.
   */
  @Test
  public void testCheckpointApplicationWindowCountAtMostOnce() throws Exception
  {
    testCheckpointApplicationWindowCount(ProcessingMode.AT_MOST_ONCE);
  }

  private void testCheckpointApplicationWindowCount(ProcessingMode processingMode) throws Exception
  {
    final long timeoutMillis = 10000L;
    final long sleepTime = 25L;

    WindowGenerator windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"), 1024);
    long resetWindow = 0L;
    long firstWindowMillis = 1448909287863L;
    int windowWidth = 100;

    windowGenerator.setResetWindow(resetWindow);
    windowGenerator.setFirstWindow(firstWindowMillis);
    windowGenerator.setWindowWidth(windowWidth);
    windowGenerator.setCheckpointCount(1, 0);

    GenericOperator go = new GenericOperator();

    DefaultAttributeMap dam = new DefaultAttributeMap();
    dam.put(OperatorContext.APPLICATION_WINDOW_COUNT, 5);
    dam.put(OperatorContext.CHECKPOINT_WINDOW_COUNT, 5);
    dam.put(OperatorContext.PROCESSING_MODE, processingMode);

    DelayAsyncFSStorageAgent storageAgent = new DelayAsyncFSStorageAgent(testMeta.getDir(), new Configuration());
    storageAgent.setDelayMS(200L);

    dam.put(OperatorContext.STORAGE_AGENT, storageAgent);

    TestStatsOperatorContext operatorContext = new TestStatsOperatorContext(0, dam, null);
    final GenericNode gn = new GenericNode(go, operatorContext);
    gn.setId(1);

    TestSink testSink = new TestSink();

    gn.connectInputPort("ip1", windowGenerator.acquireReservoir(String.valueOf(gn.id), 1024));
    gn.connectOutputPort("output", testSink);
    gn.firstWindowMillis = firstWindowMillis;
    gn.windowWidthMillis = windowWidth;

    windowGenerator.activate(null);

    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        gn.activate();
        gn.run();
        gn.deactivate();
      }
    };

    t.start();

    long startTime = System.currentTimeMillis();
    long endTime = 0;

    while (operatorContext.checkpoints.size() < 8 && ((endTime = System.currentTimeMillis()) - startTime) < timeoutMillis) {
      Thread.sleep(sleepTime);
    }

    gn.shutdown();
    t.join();

    windowGenerator.deactivate();

    Assert.assertTrue(!operatorContext.checkpoints.isEmpty());

    for (int index = 0; index < operatorContext.checkpoints.size(); index++) {
      if (operatorContext.checkpoints.get(index) == null) {
        continue;
      }

      Assert.assertEquals(0, operatorContext.checkpoints.get(index).applicationWindowCount);
      Assert.assertEquals(0, operatorContext.checkpoints.get(index).checkpointWindowCount);
    }
  }

  @Test
  public void testDefaultCheckPointDistance() throws InterruptedException
  {
    testCheckpointDistance(Context.DAGContext.CHECKPOINT_WINDOW_COUNT.defaultValue, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT.defaultValue);
  }

  @Test
  public void testDAGGreaterCheckPointDistance() throws InterruptedException
  {
    testCheckpointDistance(7, 5);
  }

  @Test
  public void testOpGreaterCheckPointDistance() throws InterruptedException
  {
    testCheckpointDistance(3, 5);
  }

  private void testCheckpointDistance(int dagCheckPoint, int opCheckPoint) throws InterruptedException
  {
    int windowWidth = 50;
    long sleeptime = 25L;
    int maxWindows = 60;
    // Adding some extra time for the windows to finish
    long maxSleep = windowWidth * maxWindows + 5000;

    ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, "default");
    final WindowGenerator windowGenerator = new WindowGenerator(executorService, 1024);
    windowGenerator.setWindowWidth(windowWidth);
    windowGenerator.setFirstWindow(executorService.getCurrentTimeMillis());
    windowGenerator.setCheckpointCount(dagCheckPoint, 0);
    //GenericOperator go = new GenericOperator();
    CheckpointDistanceOperator go = new CheckpointDistanceOperator();
    go.maxWindows = maxWindows;

    List<Integer> checkpoints = new ArrayList<Integer>();

    int window = 0;
    while (window < maxWindows) {
      window = (int)Math.ceil((double)(window + 1) / dagCheckPoint) * dagCheckPoint;
      window = (int)Math.ceil((double)window / opCheckPoint) * opCheckPoint;
      checkpoints.add(window);
    }

    final StreamContext stcontext = new StreamContext("s1");
    DefaultAttributeMap attrMap = new DefaultAttributeMap();
    attrMap.put(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, dagCheckPoint);
    attrMap.put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, opCheckPoint);
    final OperatorContext context = new com.datatorrent.stram.engine.OperatorContext(0, attrMap, null);
    final GenericNode gn = new GenericNode(go, context);
    gn.setId(1);

    //DefaultReservoir reservoir1 = new DefaultReservoir("ip1Res", 1024);
    //DefaultReservoir reservoir2 = new DefaultReservoir("ip2Res", 1024);

    //gn.connectInputPort("ip1", reservoir1);
    //gn.connectInputPort("ip2", reservoir2);
    gn.connectInputPort("ip1", windowGenerator.acquireReservoir("ip1", 1024));
    gn.connectInputPort("ip2", windowGenerator.acquireReservoir("ip2", 1024));
    gn.connectOutputPort("op", Sink.BLACKHOLE);

    final AtomicBoolean ab = new AtomicBoolean(false);
    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        gn.setup(context);
        windowGenerator.activate(stcontext);
        gn.activate();
        ab.set(true);
        gn.run();
        windowGenerator.deactivate();
        gn.deactivate();
        gn.teardown();
      }
    };
    t.start();

    long interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((go.numWindows < maxWindows) && (interval < maxSleep));

    Assert.assertEquals("Number distances", maxWindows, go.numWindows);
    int chkindex = 0;
    int nextCheckpoint = checkpoints.get(chkindex++);
    for (int i = 0; i < maxWindows; ++i) {
      if ((i + 1) > nextCheckpoint) {
        nextCheckpoint = checkpoints.get(chkindex++);
      }
      Assert.assertEquals("Windows from checkpoint for " + i, nextCheckpoint - i, (int)go.distances.get(i));
    }

    gn.shutdown();
    t.join();
  }

  private static final Logger LOG = LoggerFactory.getLogger(GenericNodeTest.class);
}
