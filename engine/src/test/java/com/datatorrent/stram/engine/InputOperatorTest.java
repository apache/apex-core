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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;

/**
 *
 */
public class InputOperatorTest
{
  static HashMap<String, List<?>> collections = new HashMap<>();
  static AtomicInteger tupleCount = new AtomicInteger();

  public static class EvenOddIntegerGeneratorInputOperator implements InputOperator, com.datatorrent.api.Operator.ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<Integer> even = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<Integer> odd = new DefaultOutputPort<>();
    private final transient CircularBuffer<Integer> evenBuffer = new CircularBuffer<>(1024);
    private final transient CircularBuffer<Integer> oddBuffer = new CircularBuffer<>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("Integer Emitter")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null) {
              (i % 2 == 0 ? evenBuffer : oddBuffer).put(i++);
              Thread.sleep(20);
            }
          } catch (InterruptedException ie) {
            // break out
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = evenBuffer.size(); i-- > 0;) {
        even.emit(evenBuffer.pollUnsafe());
      }
      for (int i = oddBuffer.size(); i-- > 0;) {
        odd.emit(oddBuffer.pollUnsafe());
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient CollectorInputPort<T> even = new CollectorInputPort<>("even", this);
    public final transient CollectorInputPort<T> odd = new CollectorInputPort<>("odd", this);
  }

  @Test
  public void testSomeMethod() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    String testWorkDir = new File("target").getAbsolutePath();
    dag.setAttribute(OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(testWorkDir, null));
    EvenOddIntegerGeneratorInputOperator generator = dag.addOperator("NumberGenerator", EvenOddIntegerGeneratorInputOperator.class);
    final CollectorModule<Number> collector = dag.addOperator("NumberCollector", new CollectorModule<Number>());

    dag.addStream("EvenIntegers", generator.even, collector.even).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("OddIntegers", generator.odd, collector.odd).setLocality(Locality.CONTAINER_LOCAL);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        return tupleCount.get() > 2;
      }
    };
    StramTestSupport.awaitCompletion(c, 2000);

    lc.shutdown();

    Assert.assertEquals("Collections size", 2, collections.size());
    Assert.assertFalse("Zero tuple count", collections.get(collector.even.id).isEmpty() && collections.get(collector.odd.id).isEmpty());
    Assert.assertTrue("Tuple count", collections.get(collector.even.id).size() - collections.get(collector.odd.id).size() <= 1);
  }

  public static class CollectorInputPort<T> extends DefaultInputPort<T>
  {
    ArrayList<T> list;
    final String id;

    public CollectorInputPort(String id, Operator module)
    {
      super();
      this.id = id;
    }

    @Override
    public void process(T tuple)
    {
      list.add(tuple);
      tupleCount.incrementAndGet();
    }

    @Override
    public void setConnected(boolean flag)
    {
      if (flag) {
        collections.put(id, list = new ArrayList<>());
      }
    }
  }
}
