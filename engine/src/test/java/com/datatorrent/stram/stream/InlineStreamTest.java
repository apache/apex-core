/**
 * Copyright (c) 2012-2013 DataTorrent, Inc. All rights reserved.
 */
package com.datatorrent.stram.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.DefaultReservoir;
import com.datatorrent.stram.engine.GenericNode;
import com.datatorrent.stram.engine.Node;
import com.datatorrent.stram.engine.OperatorContext;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.support.StramTestSupport;
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

    final PassThroughNode<Object> operator1 = new PassThroughNode<Object>();
    final GenericNode node1 = new GenericNode(operator1);
    node1.setId(1);
    operator1.setup(new OperatorContext(0, null, null, null));

    final PassThroughNode<Object> operator2 = new PassThroughNode<Object>();
    final GenericNode node2 = new GenericNode(operator2);
    node2.setId(2);
    operator2.setup(new OperatorContext(0, null, null, null));

    StreamContext streamContext = new StreamContext("node1->node2");
    InlineStream stream = new InlineStream(1024);
    stream.setup(streamContext);

    node1.connectOutputPort("output", stream);
    node2.connectInputPort("input", stream);

    prev = null;
    Sink<Object> sink = new Sink<Object>()
    {
      /**
       *
       * @param t the value of t
       */
      @Override
      public void put(Object payload)
      {
        if (payload instanceof Tuple) {
          return;
        }

        if (prev == null) {
          prev = payload;
        }
        else {
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

    DefaultReservoir reservoir1 = new DefaultReservoir("input", 1024 * 5);
    node1.connectInputPort("input", reservoir1);

    Map<Integer, Node<?>> activeNodes = new ConcurrentHashMap<Integer, Node<?>>();
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

    Assert.assertTrue("last tuple", prev != null && totalTupleCount - Integer.valueOf(prev.toString()) == 1);
    Assert.assertEquals("active operators", 2, activeNodes.size());

    stream.deactivate();
    for (Node<?> node : activeNodes.values()) {
      node.deactivate();
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
        OperatorContext ctx = new OperatorContext(id, Thread.currentThread(),
                                                  new AttributeMap.DefaultAttributeMap(Context.OperatorContext.class),
                                                  null);
        activeNodes.put(ctx.getId(), node);
        node.activate(ctx);
        activeNodes.remove(ctx.getId());
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
    public final DefaultOutputPort<T> output = new DefaultOutputPort<T>();
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

  private static final Logger logger = LoggerFactory.getLogger(InlineStreamTest.class);
}
