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
import java.util.ArrayList;
import java.util.Set;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.api.StorageAgent;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.FSStorageAgent;
import com.datatorrent.common.util.ScheduledThreadPoolExecutor;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.engine.GenericNodeTest.GenericOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 *
 */
public class NodeTest
{
  static class TestGenericOperator implements Operator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  static class TestInputOperator implements InputOperator
  {
    static int beginWindows;
    static int endWindows;

    @Override
    public void emitTuples()
    {
    }

    @Override
    public void beginWindow(long windowId)
    {
      beginWindows++;
    }

    @Override
    public void endWindow()
    {
      endWindows++;
    }

    @Override
    public void setup(OperatorContext context)
    {
      beginWindows = 0;
      endWindows = 0;
    }

    @Override
    public void teardown()
    {
    }

  }

  public NodeTest()
  {
  }

  @Ignore
  @Test
  public void testStreamingWindowGenericNode() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.getAttributes().put(LogicalPlan.STREAMING_WINDOW_SIZE_MILLIS, 10);
    dag.addOperator("GenericOperator", new TestGenericOperator());

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(2000);
  }

  @Stateless
  public static class StatelessOperator implements Operator
  {
    @Override
    public void beginWindow(long windowId)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void endWindow()
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void setup(OperatorContext context)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void teardown()
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

  }

  public static class StorageAgentImpl implements StorageAgent
  {
    static class Call
    {
      Call(String calltype, int operatorId, long windowId)
      {
      }

    }

    static final ArrayList<Call> calls = new ArrayList<>();

    @Override
    public void save(Object object, int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("getSaveStream", operatorId, windowId));
    }

    @Override
    public Object load(int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("getLoadStream", operatorId, windowId));
      return null;
    }

    @Override
    public void delete(int operatorId, long windowId) throws IOException
    {
      calls.add(new Call("delete", operatorId, windowId));
    }

    @Override
    public long[] getWindowIds(int operatorId) throws IOException
    {
      calls.add(new Call("getWindowsIds", operatorId, 0));
      return new long[0];
    }

  }

  @Test
  public void testStatelessOperatorCheckpointing()
  {
    DefaultAttributeMap attributeMap = new DefaultAttributeMap();
    attributeMap.put(OperatorContext.STORAGE_AGENT, new StorageAgentImpl());
    attributeMap.put(OperatorContext.STATELESS, true);
    Node<StatelessOperator> node = new Node<StatelessOperator>(new StatelessOperator(),
        new com.datatorrent.stram.engine.OperatorContext(0,"operator", attributeMap, null))
    {
      @Override
      public void connectInputPort(String port, SweepableReservoir reservoir)
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void run()
      {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

    };

    node.activate();

    synchronized (StorageAgentImpl.calls) {
      StorageAgentImpl.calls.clear();
      node.checkpoint(0);
      Assert.assertEquals("Calls to StorageAgent", 0, StorageAgentImpl.calls.size());
    }

    node.deactivate();
  }

  @Test
  public void testOperatorCheckpointing()
  {
    DefaultAttributeMap attributeMap = new DefaultAttributeMap();
    attributeMap.put(OperatorContext.STORAGE_AGENT, new StorageAgentImpl());
    Node<TestGenericOperator> node = new Node<TestGenericOperator>(new TestGenericOperator(),
        new com.datatorrent.stram.engine.OperatorContext(0, "operator", attributeMap, null))
    {
      @Override
      public void connectInputPort(String port, SweepableReservoir reservoir)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public void run()
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

    };

    node.activate();

    synchronized (StorageAgentImpl.calls) {
      StorageAgentImpl.calls.clear();
      node.checkpoint(0);
      Assert.assertEquals("Calls to StorageAgent", 1, StorageAgentImpl.calls.size());
    }

    node.deactivate();
  }

  @SuppressWarnings("SleepWhileInLoop")
  public static void testDoubleCheckpointHandling(ProcessingMode processingMode, boolean trueGenericFalseInput, String path)
      throws Exception
  {
    WindowGenerator windowGenerator = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"), 1024);
    windowGenerator.setResetWindow(0L);
    windowGenerator.setFirstWindow(0L);
    windowGenerator.setWindowWidth(100);
    windowGenerator.setCheckpointCount(1, 0);

    CheckpointTestOperator checkpointTestOperator;

    if (trueGenericFalseInput) {
      checkpointTestOperator = new CheckpointTestOperator();
    } else {
      checkpointTestOperator = new InputCheckpointTestOperator();
    }
    DefaultAttributeMap dam = new DefaultAttributeMap();
    dam.put(com.datatorrent.stram.engine.OperatorContext.APPLICATION_WINDOW_COUNT, 2);
    dam.put(com.datatorrent.stram.engine.OperatorContext.CHECKPOINT_WINDOW_COUNT, 2);
    dam.put(com.datatorrent.stram.engine.OperatorContext.PROCESSING_MODE, processingMode);
    dam.put(com.datatorrent.stram.engine.OperatorContext.STORAGE_AGENT, new FSStorageAgent(path, new Configuration()));

    final Node in;

    if (trueGenericFalseInput) {
      in = new GenericNode(checkpointTestOperator, new com.datatorrent.stram.engine.OperatorContext(0, "operator", dam, null));
    } else {
      in = new InputNode((InputCheckpointTestOperator)checkpointTestOperator, new com.datatorrent.stram.engine.OperatorContext(0, "operator",
          dam, null));
    }

    in.setId(1);

    TestSink testSink = new TestSink();
    String inputPort;

    if (trueGenericFalseInput) {
      inputPort = "ip1";
    } else {
      inputPort = Node.INPUT;
    }

    in.connectInputPort(inputPort, windowGenerator.acquireReservoir(String.valueOf(in.id), 1024));
    in.connectOutputPort("output", testSink);
    in.firstWindowMillis = 0;
    in.windowWidthMillis = 100;

    windowGenerator.activate(null);

    Thread t = new Thread()
    {
      @Override
      public void run()
      {
        in.activate();
        in.run();
        in.deactivate();
      }
    };

    t.start();

    long startTime = System.currentTimeMillis();
    long endTime = 0;

    while (checkpointTestOperator.numWindows < 3 && ((endTime = System.currentTimeMillis()) - startTime) < 6000) {
      Thread.sleep(50);
    }

    in.shutdown();
    t.join();

    windowGenerator.deactivate();

    Assert.assertFalse(checkpointTestOperator.checkpointTwice);
    Assert.assertTrue("Timed out", (endTime - startTime) < 5000);
  }

  private static class CheckpointTestOperator extends GenericOperator implements CheckpointNotificationListener
  {
    public Set<Long> checkpointedWindows = Sets.newHashSet();
    public volatile boolean checkpointTwice = false;
    public volatile int numWindows = 0;

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

  private static class InputCheckpointTestOperator extends CheckpointTestOperator implements InputOperator
  {
    @Override
    public void emitTuples()
    {
    }
  }
}
