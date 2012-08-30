/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
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
public class InlineStreamTest
{
  private static Logger LOG = LoggerFactory.getLogger(InlineStreamTest.class);
  private Object prev;

  @Test
  public void test() throws Exception
  {
    final int totalTupleCount = 5000;
    prev = null;

    final AbstractNode node1 = new PassThroughNode();
    node1.setup(new NodeConfiguration(null));

    final AbstractNode node2 = new PassThroughNode();
    node2.setup(new NodeConfiguration(null));

    InlineStream stream = new InlineStream();
    stream.setup(new StreamConfiguration());

    Sink sink = stream.connect("node1.output", node1);
    node1.connect("output", sink);

    sink = node2.connect("input", stream);
    stream.connect("node2.input", sink);

    sink = new Sink()
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
    node2.connect("output", sink);

    sink = node1.connect("input", new Sink()
    {
      // we just needed some random sink
      @Override
      public void process(Object payload)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    });

    StreamContext streamContext = new StreamContext("node1->node2");

    stream.activate(streamContext);

    Map<String, Thread> activeNodes = new ConcurrentHashMap<String, Thread>();
    launchNodeThreads(Arrays.asList(node1, node2), activeNodes);

    for (int i = 0; i < totalTupleCount; i++) {
      sink.process(i);
    }

    synchronized (this) {
      this.wait(1500 + totalTupleCount / 500);
    }

    Assert.assertTrue("last tuple", prev != null && totalTupleCount - Integer.valueOf(prev.toString()) == 1);
    Assert.assertEquals("active nodes", 2, activeNodes.size());

    node2.deactivate();
    node1.deactivate();
    stream.deactivate();

    node2.teardown();
    node1.teardown();
    stream.teardown();

    Assert.assertEquals("active nodes", 0, activeNodes.size());
  }

  private void launchNodeThreads(Collection<? extends AbstractNode> nodes, final Map<String, Thread> activeNodes)
  {
    int i = 1;
    for (final AbstractNode node: nodes) {
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
  @NodeAnnotation(ports = {
    @PortAnnotation(name = "input", type = PortType.INPUT),
    @PortAnnotation(name = "output", type = PortType.OUTPUT)
  })
  public static class PassThroughNode extends AbstractNode
  {
    private String nodeId;
    private boolean logMessages = false;

    @Override
    public void setup(NodeConfiguration config)
    {
      nodeId = config.get("NodeId", null);
      super.setup(config);
    }

    public boolean isAppendNodeId()
    {
      return nodeId != null;
    }

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
      if (nodeId == null) {
        emit("output", o);
      }
      else {
        emit("output", nodeId.concat(" > ").concat(o.toString()));
      }
    }
  }
}
