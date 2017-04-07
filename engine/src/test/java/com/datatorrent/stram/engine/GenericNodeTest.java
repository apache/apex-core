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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.Sink;
import com.datatorrent.api.Stats.OperatorStats;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.ScheduledExecutorService;
import com.datatorrent.common.util.ScheduledThreadPoolExecutor;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.stram.CustomControlTupleTest;
import com.datatorrent.stram.api.Checkpoint;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.stream.BufferServerPublisher;
import com.datatorrent.stram.stream.BufferServerSubscriber;
import com.datatorrent.stram.stream.OiOStream;
import com.datatorrent.stram.tuple.CustomControlTuple;
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

    public TestStatsOperatorContext(int id, String name, AttributeMap attributes, Context parentContext)
    {
      super(id, name, attributes, parentContext);
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
    DefaultOutputPort<Object> op = new DefaultOutputPort<>();

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
    List<Integer> distances = new ArrayList<>();
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

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testSynchingLogic() throws InterruptedException
  {
    long sleeptime = 25L;
    final ArrayList<Object> list = new ArrayList<>();
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
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
  public void testBufferServerSubscriberActivationBeforeOperator() throws InterruptedException, IOException
  {
    final String streamName = "streamName";
    final String upstreamNodeId = "upstreamNodeId";
    final String  downstreamNodeId = "downStreamNodeId";

    EventLoop eventloop = DefaultEventLoop.createEventLoop("StreamTestEventLoop");

    ((DefaultEventLoop)eventloop).start();
    final Server bufferServer = new Server(eventloop, 0); // find random port
    final int bufferServerPort = bufferServer.run().getPort();

    final StreamCodec<Object> serde = new DefaultStatefulStreamCodec<>();
    final BlockingQueue<Object> tuples = new ArrayBlockingQueue<>(10);

    GenericTestOperator go = new GenericTestOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
    gn.setId(1);

    Sink<Object> output = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        tuples.add(tuple);
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }
    };

    InetSocketAddress socketAddress = new InetSocketAddress("localhost", bufferServerPort);

    StreamContext issContext = new StreamContext(streamName);
    issContext.setSourceId(upstreamNodeId);
    issContext.setSinkId(downstreamNodeId);
    issContext.setFinishedWindowId(-1);
    issContext.setBufferServerAddress(socketAddress);
    issContext.put(StreamContext.CODEC, serde);
    issContext.put(StreamContext.EVENT_LOOP, eventloop);

    StreamContext ossContext = new StreamContext(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);
    ossContext.setBufferServerAddress(socketAddress);
    ossContext.put(StreamContext.CODEC, serde);
    ossContext.put(StreamContext.EVENT_LOOP, eventloop);

    BufferServerPublisher oss = new BufferServerPublisher(upstreamNodeId, 1024);
    oss.setup(ossContext);
    oss.activate(ossContext);

    oss.put(new Tuple(MessageType.BEGIN_WINDOW, 0x1L));
    byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
    buff[buff.length - 1] = (byte)1;
    oss.put(buff);
    oss.put(new EndWindowTuple(0x1L));
    oss.put(new Tuple(MessageType.BEGIN_WINDOW, 0x2L));
    buff = PayloadTuple.getSerializedTuple(0, 1);
    buff[buff.length - 1] = (byte)2;
    oss.put(buff);
    oss.put(new EndWindowTuple(0x2L));
    oss.put(new Tuple(MessageType.BEGIN_WINDOW, 0x3L));
    buff = PayloadTuple.getSerializedTuple(0, 1);
    buff[buff.length - 1] = (byte)3;
    oss.put(buff);

    oss.put(new EndWindowTuple(0x3L));
    oss.put(new EndStreamTuple(0L));

    BufferServerSubscriber iss = new BufferServerSubscriber(downstreamNodeId, 1024);
    iss.setup(issContext);

    gn.connectInputPort(GenericTestOperator.IPORT1, iss.acquireReservoir("testReservoir", 10));
    gn.connectOutputPort(GenericTestOperator.OPORT1, output);

    SweepableReservoir tupleWait = iss.acquireReservoir("testReservoir2", 10);

    iss.activate(issContext);

    while (tupleWait.sweep() == null) {
      Thread.sleep(100);
    }

    gn.firstWindowMillis = 0;
    gn.windowWidthMillis = 100;

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
    t.join();

    Assert.assertEquals(10, tuples.size());

    List<Object> list = new ArrayList<>(tuples);

    Assert.assertEquals("Payload Tuple 1", 1, ((byte[])list.get(1))[5]);
    Assert.assertEquals("Payload Tuple 2", 2, ((byte[])list.get(4))[5]);
    Assert.assertEquals("Payload Tuple 3", 3, ((byte[])list.get(7))[5]);

    if (bufferServer != null) {
      bufferServer.stop();
    }

    ((DefaultEventLoop)eventloop).stop();
  }

  @Test
  public void testPrematureTermination() throws InterruptedException
  {
    long maxSleep = 5000;
    long sleeptime = 25L;
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
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
  public void testControlTuplesDeliveryGenericNode() throws InterruptedException
  {
    long maxSleep = 5000000;
    long sleeptime = 25L;
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
    gn.setId(1);
    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("ip1Res", 1024);

    gn.connectInputPort("ip1", reservoir1);
    TestSink testSink = new TestSink();
    gn.connectOutputPort("op", testSink);
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
    Tuple beginWindow = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);
    reservoir1.add(beginWindow);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));
    controlTupleCount = gn.controlTupleCount;

    CustomControlTuple t1 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(1, false));
    CustomControlTuple t2 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(2, true));
    CustomControlTuple t3 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(3, false));
    CustomControlTuple t4 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(4, true));
    reservoir1.add(t1);
    reservoir1.add(t2);
    reservoir1.add(t3);
    reservoir1.add(t4);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));

    Assert.assertTrue("Custom control tuples emitted immediately", testSink.getResultCount() == 3);

    controlTupleCount = gn.controlTupleCount;
    Tuple endWindow = new Tuple(MessageType.END_WINDOW, 0x1L);
    reservoir1.add(endWindow);

    interval = 0;
    do {
      Thread.sleep(sleeptime);
      interval += sleeptime;
    } while ((gn.controlTupleCount == controlTupleCount) && (interval < maxSleep));

    gn.shutdown();
    t.join();

    Assert.assertTrue("Total control tuples", testSink.getResultCount() == 6);

    long expected = 0;
    for (Object o: testSink.collectedTuples) {
      if (o instanceof CustomControlTuple) {
        expected++;
      }
    }
    Assert.assertTrue("Number of Custom control tuples", expected == 4);
  }

  @Test
  public void testControlTuplesDeliveryOiONode() throws InterruptedException
  {
    GenericOperator go = new GenericOperator();
    final OiONode oioNode = new OiONode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
    oioNode.setId(1);

    OiOStream stream = new OiOStream();
    SweepableReservoir reservoir = stream.getReservoir();
    ((OiOStream.OiOReservoir)reservoir).setControlSink((oioNode).getControlSink(reservoir));
    oioNode.connectInputPort("ip1", reservoir);
    Sink controlSink = oioNode.getControlSink(reservoir);

    TestSink testSink = new TestSink();
    oioNode.connectOutputPort("op", testSink);
    oioNode.firstWindowMillis = 0;
    oioNode.windowWidthMillis = 100;

    oioNode.activate();

    Tuple beginWindow = new Tuple(MessageType.BEGIN_WINDOW, 0x1L);
    controlSink.put(beginWindow);
    Assert.assertTrue("Begin window", testSink.getResultCount() == 1);

    CustomControlTuple t1 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(1, false));
    CustomControlTuple t2 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(2, true));
    CustomControlTuple t3 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(3, false));
    CustomControlTuple t4 = new CustomControlTuple(new CustomControlTupleTest.TestControlTuple(4, true));
    controlSink.put(t1);
    controlSink.put(t2);
    controlSink.put(t3);
    controlSink.put(t4);
    Assert.assertTrue("Custom control tuples emitted immediately", testSink.getResultCount() == 3);

    Tuple endWindow = new Tuple(MessageType.END_WINDOW, 0x1L);
    controlSink.put(endWindow);

    oioNode.deactivate();
    oioNode.shutdown();

    Assert.assertTrue("Total control tuples", testSink.getResultCount() == 6);

    long expected = 0;
    for (Object o: testSink.collectedTuples) {
      if (o instanceof CustomControlTuple) {
        expected++;
      }
    }
    Assert.assertTrue("Number of Custom control tuples", expected == 4);
  }

  @Test
  public void testReservoirPortMapping() throws InterruptedException
  {
    long maxSleep = 5000;
    long sleeptime = 25L;
    GenericOperator go = new GenericOperator();
    final GenericNode gn = new GenericNode(go, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
        new DefaultAttributeMap(), null));
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

    gn.populateReservoirInputPortMap();

    gn.shutdown();
    t.join();

    Assert.assertTrue("Port Mapping Size", gn.reservoirPortMap.size() == 2);
    Assert.assertTrue("Sink 1 is not a port", gn.reservoirPortMap.get(reservoir1) instanceof Operator.InputPort);
    Assert.assertTrue("Sink 2 is not a port", gn.reservoirPortMap.get(reservoir2) instanceof Operator.InputPort);
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

    TestStatsOperatorContext operatorContext = new TestStatsOperatorContext(0, "operator", dam, null);
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

    List<Integer> checkpoints = new ArrayList<>();

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
    final OperatorContext context = new com.datatorrent.stram.engine.OperatorContext(0, "operator", attrMap, null);
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
