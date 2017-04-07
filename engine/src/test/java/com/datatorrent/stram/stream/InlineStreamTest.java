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
package com.datatorrent.stram.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.engine.AbstractReservoir;
import com.datatorrent.stram.engine.GenericNode;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import com.datatorrent.stram.tuple.Tuple;

/**
 * Test for message flow through DAG
 */
public class InlineStreamTest
{
  private Object prev;

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void test() throws Exception
  {
    final int totalTupleCount = 5000;

    final PassThroughNode<Object> operator1 = new PassThroughNode<>();
    final GenericNode node1 = new GenericNode(operator1, new OperatorContext(1, "operator1", new DefaultAttributeMap(),
        null));
    node1.setId(1);
    operator1.setup(node1.context);

    final PassThroughNode<Object> operator2 = new PassThroughNode<>();
    final GenericNode node2 = new GenericNode(operator2, new OperatorContext(2, "operator2", new DefaultAttributeMap(),
        null));
    node2.setId(2);
    operator2.setup(node2.context);

    StreamContext streamContext = new StreamContext("node1->node2");
    final InlineStream stream = new InlineStream(1024);
    stream.setup(streamContext);

    node1.connectOutputPort("output", stream);
    node2.connectInputPort("input", stream.getReservoir());

    prev = null;
    Sink<Object> sink = new Sink<Object>()
    {
      @Override
      public void put(Object payload)
      {
        if (payload instanceof Tuple) {
          return;
        }

        if (prev == null) {
          prev = payload;
        } else {
          if (Integer.valueOf(payload.toString()) - Integer.valueOf(prev.toString()) != 1) {
            synchronized (InlineStreamTest.this) {
              InlineStreamTest.this.notify();
            }
          }

          prev = payload;
        }

        if (Integer.valueOf(prev.toString()) == totalTupleCount - 1) {
          synchronized (InlineStreamTest.this) {
            InlineStreamTest.this.notify();
          }
        }
      }

      @Override
      public int getCount(boolean reset)
      {
        return 0;
      }

    };
    node2.connectOutputPort("output", sink);

    AbstractReservoir reservoir1 = AbstractReservoir.newReservoir("input", 1024 * 5);
    node1.connectInputPort("input", reservoir1);

    Map<Integer, Node<?>> activeNodes = new ConcurrentHashMap<>();
    launchNodeThread(node1, activeNodes);
    launchNodeThread(node2, activeNodes);
    stream.activate(streamContext);

    reservoir1.put(StramTestSupport.generateBeginWindowTuple("irrelevant", 0));
    for (int i = 0; i < totalTupleCount; i++) {
      reservoir1.put(i);
    }
    reservoir1.put(StramTestSupport.generateEndWindowTuple("irrelevant", 0));


    synchronized (this) {
      this.wait(200);
    }

    Assert.assertNotNull(prev);
    Assert.assertEquals("processing complete", totalTupleCount, Integer.valueOf(prev.toString()) + 1);
    Assert.assertEquals("active operators", 2, activeNodes.size());

    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        final SweepableReservoir reservoir = stream.getReservoir();
        logger.debug("stream {} empty {}, size {}", stream, reservoir.isEmpty(), reservoir.size(false));
        return reservoir.isEmpty();
      }
    };

    Assert.assertTrue("operator should finish processing all events within 1 second", StramTestSupport.awaitCompletion(c, 1000));

    stream.deactivate();
    for (Node<?> node : activeNodes.values()) {
      node.shutdown();
    }

    for (int i = 0; i < 10; i++) {
      Thread.sleep(20);
      if (activeNodes.isEmpty()) {
        break;
      }
    }

    stream.teardown();
    operator2.teardown();
    operator1.teardown();

    Assert.assertEquals("active operators", 0, activeNodes.size());
  }

  final AtomicInteger counter = new AtomicInteger(0);

  private void launchNodeThread(final Node<?> node, final Map<Integer, Node<?>> activeNodes)
  {
    Runnable nodeRunnable = new Runnable()
    {
      @Override
      public void run()
      {
        int id = counter.incrementAndGet();
        activeNodes.put(id, node);
        node.activate();
        node.run();
        node.deactivate();
        activeNodes.remove(id);
      }

    };

    Thread launchThread = new Thread(nodeRunnable);
    launchThread.start();
  }

  /**
   * Operator implementation that simply passes on any tuple received
   *
   * @param <T>
   */
  public static class PassThroughNode<T> extends BaseOperator
  {
    public final DefaultInputPort<T> input = new DefaultInputPort<T>()
    {
      @Override
      public void process(T tuple)
      {
        output.emit(tuple);
      }

    };
    public final DefaultOutputPort<T> output = new DefaultOutputPort<>();
    private boolean logMessages = false;

    public boolean isLogMessages()
    {
      return logMessages;
    }

    public void setLogMessages(boolean logMessages)
    {
      this.logMessages = logMessages;
    }

  }

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(InlineStreamTest.class);
}
