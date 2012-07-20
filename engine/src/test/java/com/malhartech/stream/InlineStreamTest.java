/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for message flow through DAG
 */
public class InlineStreamTest {
  private static Logger LOG = LoggerFactory.getLogger(InlineStreamTest.class);
  private Object prev;
  
  @Test
  public void test() throws Exception {

      final Object s = new Object();
      final int totalTupleCount = 5000;
      prev = null;
      
      Sink node2Sink = new Sink() {
        @Override
        public void doSomething(Tuple t) {
          if (prev == null) {
            prev = t.getObject();
          }
          else {
            if (Integer.valueOf(t.getObject().toString()) - Integer.valueOf(prev.toString()) != 1) {
              LOG.info("Got the tuples out of order!");
              LOG.info(prev + " followed by " + t.getObject());
              synchronized (s) {
                s.notify();
              }
            }
            prev = t.getObject();
          }
          
          if (Integer.valueOf(t.getObject().toString()) == totalTupleCount - 1) {
            LOG.info("last tuple received.");
            synchronized (s) {
              s.notify();
            }
          }          
        }
      };
    
      NodeContext ctx = new NodeContext("1");
      AbstractNode node1 = new PassThroughNode(ctx);

      AbstractNode node2 = new PassThroughNode(new NodeContext("2"));
      
      InlineStream stream12 = new InlineStream();
      StreamContext sc1 = new StreamContext();
      sc1.setSink(node2.getSink(sc1));
      stream12.setContext(sc1);
       
      node1.addOutputStream(sc1);
      
      StreamContext sc2 = new StreamContext();
      sc1.setSink(node2Sink);
      node2.addOutputStream(sc2);

      Map<String, Thread> activeNodes = new ConcurrentHashMap<String, Thread>();
      launchNodeThreads(Arrays.asList(node1, node2), activeNodes);

      StreamContext streamContext = new StreamContext();
      streamContext.setSink(node1.getSink(streamContext));
      
      for (int i=0; i<totalTupleCount; i++) {
        node1.getSink(streamContext).doSomething(StramTestSupport.generateTuple(i, 0, streamContext));
      }

      synchronized(s) {
        s.wait(1500 + totalTupleCount/500);
      }

      Assert.assertTrue("last tuple", prev != null &&  totalTupleCount - Integer.valueOf(prev.toString()) == 1);
      Assert.assertEquals("active nodes", 2, activeNodes.size());
      
      node1.stopSafely();
      node2.stopSafely();
      Thread.sleep(100);
      Assert.assertEquals("active nodes", 0, activeNodes.size());
      
  }
  
  private void launchNodeThreads(Collection<? extends AbstractNode> nodes, final Map<String, Thread> activeNodes) {
    for (final AbstractNode node : nodes) {
      // launch nodes
      Runnable nodeRunnable = new Runnable() {
        @Override
        public void run() {
          node.run();
          // processing has ended
          activeNodes.remove(node.getContext().getId());
        }
      };
      Thread launchThread = new Thread(nodeRunnable);
      activeNodes.put(node.getContext().getId(), launchThread);
      launchThread.start();
    }
  }
  
  /**
   * Node implementation that simply passes on any tuple received
   */
  public static class PassThroughNode extends AbstractNode {

    private boolean appendNodeId = false;

    public boolean isAppendNodeId() {
      return appendNodeId;
    }

    public void setAppendNodeId(boolean appendNodeId) {
      LOG.info("appendNodeId=" + appendNodeId);
      this.appendNodeId = appendNodeId;
    }

    private boolean logMessages = false;
    
    public boolean isLogMessages() {
      return logMessages;
    }

    public void setLogMessages(boolean logMessages) {
      this.logMessages = logMessages;
    }

    public PassThroughNode(NodeContext ctx) {
      super(ctx);
    }

    @Override
    public void process(NodeContext context, StreamContext sc, Object o) {
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
