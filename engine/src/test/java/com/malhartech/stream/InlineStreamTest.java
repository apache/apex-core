/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.api.*;
import com.malhartech.dag.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for message flow through DAG
 */
public class InlineStreamTest
{
  private static final Logger LOG = LoggerFactory.getLogger(InlineStreamTest.class);
  private Object prev;

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void test() throws Exception
  {
    final int totalTupleCount = 5000;
    prev = null;

    final PassThroughNode node1 = new PassThroughNode();
    node1.setup(new OperatorConfiguration());

    final PassThroughNode node2 = new PassThroughNode();
    node2.setup(new OperatorConfiguration());

    InlineStream stream = new InlineStream();
    stream.setup(new StreamConfiguration());

    node1.output.setSink(stream);

    stream.setSink("node2.input", node2.input);
    node2.input.setConnected(true);

    Sink sink = new Sink()
    {
      /**
       *
       * @param t the value of t
       */
      @Override
      public void process(Object payload)
      {
        if (payload instanceof Tuple) {
          // we ignore the control tuple
        }
        else {
          if (prev == null) {
            prev = payload;
          }
          else {
            if (Integer.valueOf(payload.toString()) - Integer.valueOf(prev.toString()) != 1) {
              LOG.info("Got the tuples out of order!");
              LOG.info(prev + " followed by " + payload);
              synchronized (InlineStreamTest.this) {
                InlineStreamTest.this.notify();
              }
            }

            prev = payload;
          }

          if (Integer.valueOf(prev.toString()) == totalTupleCount - 1) {
            LOG.info("last tuple received.");
            synchronized (InlineStreamTest.this) {
              InlineStreamTest.this.notify();
            }
          }
        }
      }
    };
    node2.output.setSink(sink);

    sink = node1.input.getSink();
    node1.input.setConnected(true);

    StreamContext streamContext = new StreamContext("node1->node2");

    stream.activated(streamContext);

    Map<String, Node> activeNodes = new ConcurrentHashMap<String, Node>();
    launchNodeThread(node1, activeNodes);
    launchNodeThread(node2, activeNodes);

    for (int i = 0; i < totalTupleCount; i++) {
      sink.process(i);
    }

    synchronized (this) {
      this.wait(100);
    }

    Assert.assertTrue("last tuple", prev != null && totalTupleCount - Integer.valueOf(prev.toString()) == 1);
    Assert.assertEquals("active operators", 2, activeNodes.size());

    for (Node node: activeNodes.values()) {
      node.deactivate();
    }
    stream.deactivated();

    for (int i = 0; i < 10; i++) {
      Thread.sleep(20);
      if (activeNodes.isEmpty()) {
        break;
      }
    }

    node2.teardown();
    node1.teardown();
    stream.teardown();

    Assert.assertEquals("active operators", 0, activeNodes.size());
  }
  final AtomicInteger i = new AtomicInteger(0);

  private void launchNodeThread(final Operator operator, final Map<String, Node> activeNodes)
  {
    Runnable nodeRunnable = new Runnable()
    {
      @Override
      public void run()
      {
        Node n;
        String id = String.valueOf(i.incrementAndGet());
        if (operator instanceof SyncInputOperator) {
          n = new SyncInputNode(id, (SyncInputOperator)operator);
        }
        else if (operator instanceof AsyncInputOperator) {
          n = new AsyncInputNode(id, (AsyncInputOperator)operator);
        }
        else {
          n = new GenericNode(id, operator);
        }

        OperatorContext ctx = new OperatorContext(String.valueOf(i.incrementAndGet()), Thread.currentThread());
        activeNodes.put(ctx.getId(), n);
        n.activate(ctx);
        activeNodes.remove(ctx.getId());
      }
    };

    Thread launchThread = new Thread(nodeRunnable);
    launchThread.start();
  }

  /**
   * Operator implementation that simply passes on any tuple received
   */
  public static class PassThroughNode<T> extends BaseOperator
  {
    public final DefaultInputPort<T> input = new DefaultInputPort<T>(this)
    {
      @Override
      public void process(T tuple)
      {
        output.emit(tuple);
      }
    };
    public final DefaultOutputPort<T> output = new DefaultOutputPort(this);
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
}
