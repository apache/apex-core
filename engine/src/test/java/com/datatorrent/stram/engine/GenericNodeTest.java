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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.*;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.common.util.AsyncFSStorageAgent;
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
    public org.junit.runner.Description desc;

    public String getDir()
    {
      String methodName = desc.getMethodName();
      String className = desc.getClassName();
      return "target/" + className + "/" + methodName;
    }

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.desc = description;
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }
  }

  public static class GenericOperator implements Operator, CheckpointListener
  {
    long beginWindowId;
    long endWindowId;
    public volatile boolean checkpointed = false;

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
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void checkpointed(long windowId)
    {
      checkpointed = true;
    }

    @Override
    public void committed(long windowId)
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
    DefaultReservoir reservoir1 = new DefaultReservoir("ip1Res", 1024);
    DefaultReservoir reservoir2 = new DefaultReservoir("ip2Res", 1024);
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
    }
    while (ab.get() == false);


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
    DefaultReservoir reservoir1 = new DefaultReservoir("ip1Res", 1024);
    DefaultReservoir reservoir2 = new DefaultReservoir("ip2Res", 1024);

    gn.connectInputPort("ip1", reservoir1);
    gn.connectInputPort("ip2", reservoir2);
    gn.connectOutputPort("op", Sink.BLACKHOLE);

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
    }
    while ((ab.get() == false) && (interval < maxSleep));


    int controlTupleCount = gn.controlTupleCount;
    Tuple beginWindow1 = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);

    reservoir1.add(beginWindow1);
    reservoir2.add(beginWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    }
    while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("Begin window called", go.endWindowId != go.beginWindowId);
    controlTupleCount = gn.controlTupleCount;

    Tuple endWindow1 = new EndWindowTuple(0x1L);

    reservoir1.add(endWindow1);
    reservoir2.add(endWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    }
    while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("End window called", go.endWindowId == go.beginWindowId);
    controlTupleCount = gn.controlTupleCount;

    Tuple beginWindow2 = new Tuple(MessageType.BEGIN_WINDOW, 0x2L);

    reservoir1.add(beginWindow2);
    reservoir2.add(beginWindow2);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    }
    while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));

    gn.shutdown();
    t.join();

    Assert.assertTrue("End window not called", go.endWindowId != go.beginWindowId);
  }

  /**
   * Tests to make sure that {@link CheckpointListener#checkpointed(long) is called
   * immediately after copying to hdfs in the case of exactly once processing mode.
   * @throws Exception
   */
  @Test
  public void testCheckpointedCallTestExactlyOnce() throws Exception
  {
    asyncCheckpointedCalledHelper(ProcessingMode.EXACTLY_ONCE);
  }

  /**
   * Tests to make sure that {@link CheckpointListener#checkpointed(long) is NOT called immediately after the asynchronous
   * task of copying to hdfs is triggered, in the at least once processing mode.
   * @throws Exception
   */
  @Test
  public void testCheckpointedCallTestAtleastOnce() throws Exception
  {
    asyncCheckpointedCalledHelper(ProcessingMode.AT_LEAST_ONCE);
  }

  /**
   * Tests to make sure that {@link CheckpointListener#checkpointed(long) is NOT called immediately after the asynchronous
   * task of copying to hdfs is triggered, in the at most once processing mode.
   * @throws Exception
   */
  @Test
  public void testCheckpointedCallTestAtModeOnce() throws Exception
  {
    asyncCheckpointedCalledHelper(ProcessingMode.AT_MOST_ONCE);
  }

  private void asyncCheckpointedCalledHelper(ProcessingMode processingMode) throws Exception
  {
    long maxSleep = 5000;
    long sleeptime = 25L;
    GenericOperator go = new GenericOperator();
    AsyncFSStorageAgent storageAgent = new TestAsyncFSStorageAgent(testMeta.getDir(), new Configuration());
    DefaultAttributeMap amap = new DefaultAttributeMap();
    amap.put(OperatorContext.STORAGE_AGENT, storageAgent);
    amap.put(OperatorContext.PROCESSING_MODE, processingMode);
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, amap, null));
    gn.setId(1);
    gn.APPLICATION_WINDOW_COUNT = 1;
    gn.CHECKPOINT_WINDOW_COUNT = 1;
    DefaultReservoir reservoir1 = new DefaultReservoir("ip1Res", 1024);
    DefaultReservoir reservoir2 = new DefaultReservoir("ip2Res", 1024);

    gn.connectInputPort("ip1", reservoir1);
    gn.connectInputPort("ip2", reservoir2);
    gn.connectOutputPort("op", Sink.BLACKHOLE);

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
    }
    while ((ab.get() == false) && (interval < maxSleep));


    int controlTupleCount = gn.controlTupleCount;
    Tuple beginWindow1 = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);

    reservoir1.add(beginWindow1);
    reservoir2.add(beginWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    }
    while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("Begin window called", go.endWindowId != go.beginWindowId);
    controlTupleCount = gn.controlTupleCount;

    Tuple endWindow1 = new EndWindowTuple(0x1L);

    gn.doCheckpoint = true;

    reservoir1.add(endWindow1);
    reservoir2.add(endWindow1);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    }
    while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    Assert.assertTrue("End window called", go.endWindowId == go.beginWindowId);

    if (processingMode == ProcessingMode.EXACTLY_ONCE) {
      Assert.assertTrue(go.checkpointed);
    } else {
      Assert.assertFalse(go.checkpointed);
    }
  }

  public static class TestAsyncFSStorageAgent extends AsyncFSStorageAgent
  {
    private static final long serialVersionUID = 201511111120L;

    public TestAsyncFSStorageAgent(String path, Configuration conf)
    {
      super(path, conf);
    }

    @Override
    public void copyToHDFS(final int operatorId, final long windowId) throws IOException
    {
      //Do nothing
    }
  }
}
