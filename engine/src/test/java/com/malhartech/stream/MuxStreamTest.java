/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for message flow through DAG
 */
public class MuxStreamTest
{
  private static Logger LOG = LoggerFactory.getLogger(MuxStreamTest.class);
  private Object prev;

  @Test
  public void test() throws Exception
  {

    final Object s = new Object();
    final int totalTupleCount = 5000;
    prev = null;

    Sink node2Sink = new Sink()
    {
      /**
       *
       * @param t the value of t
       */
      @Override
      public void sink(Object t)
      {
        if (prev == null) {
          prev = t.getObject();
        }
        else if (t.getObject() != null) {
          if (Integer.valueOf(t.getObject().toString()) - Integer.valueOf(prev.toString()) != 1) {
            LOG.info("Got the tuples out of order!");
            LOG.info(prev + " followed by " + t.getObject());
            synchronized (s) {
              s.notify();
            }
          }
          prev = t.getObject();
        }

        if (t.getObject() != null && Integer.valueOf(t.getObject().toString()) == totalTupleCount - 1) {
          LOG.info("last tuple received.");
          synchronized (s) {
            s.notify();
          }
        }
      }
    };

    AbstractNode node1 = new PassThroughNode();
    AbstractNode node2 = new PassThroughNode();

    MuxStream stream12 = new MuxStream();
    StreamContext sc1 = new StreamContext("node1", "node2");
    sc1.setSink(node2.connectPort("", sc1));
//    stream12.setContext(sc1);

    node1.connectOutput(sc1);

    StreamContext sc2 = new StreamContext("node2", "node2sink");
    sc2.setSink(node2Sink);
    node2.connectOutput(sc2);

    Map<String, Thread> activeNodes = new ConcurrentHashMap<String, Thread>();
    launchNodeThreads(Arrays.asList(node1, node2), activeNodes);

    StreamContext streamContext = new StreamContext("source", "node1");

    Sink node1InputPort = node1.connectPort("", streamContext);
    streamContext.setSink(node1InputPort);

    for (int i = 0; i < totalTupleCount; i++) {
      node1InputPort.sink(StramTestSupport.generateTuple(i, 0, streamContext));
    }

    synchronized (s) {
      s.wait(1500 + totalTupleCount / 500);
    }

    Assert.assertTrue("last tuple", prev != null && totalTupleCount - Integer.valueOf(prev.toString()) == 1);
    Assert.assertEquals("active nodes", 2, activeNodes.size());

    node1.deactivate();
    node2.deactivate();
    Thread.sleep(100);
    Assert.assertEquals("active nodes", 0, activeNodes.size());

  }

  private void launchNodeThreads(Collection<? extends AbstractNode> nodes, final Map<String, Thread> activeNodes)
  {
    int i = 1;
    for (final AbstractNode node : nodes) {
      final NodeContext ctx = new NodeContext(String.valueOf(i++));
      // launch nodes
      Runnable nodeRunnable = new Runnable()
      {
        @Override
        public void run()
        {
          node.activate(ctx);
          // processing has ended
          activeNodes.remove(ctx.getId());
        }
      };
      Thread launchThread = new Thread(nodeRunnable);
      activeNodes.put(ctx.getId(), launchThread);
      launchThread.start();
    }
  }

  /**
   * Node implementation that simply passes on any tuple received
   */
  public static class PassThroughNode extends AbstractNode
  {
    private boolean appendNodeId = false;

    public boolean isAppendNodeId()
    {
      return appendNodeId;
    }

    public void setAppendNodeId(boolean appendNodeId)
    {
      LOG.info("appendNodeId=" + appendNodeId);
      this.appendNodeId = appendNodeId;
    }
    private boolean logMessages = false;

    public boolean isLogMessages()
    {
      return logMessages;
    }

    public void setLogMessages(boolean logMessages)
    {
      this.logMessages = logMessages;
    }

    @Override
    public void process(Object o)
    {
      if (appendNodeId) {
        o = this.getContext().getId() + " > " + o;
      }
      emit(o);
      if (logMessages) {
        LOG.info("emit: " + o);
      }
    }

  }
}
