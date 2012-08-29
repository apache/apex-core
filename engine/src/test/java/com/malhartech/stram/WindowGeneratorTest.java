/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.ContainerHeartbeatResponse;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;

public class WindowGeneratorTest
{

  @Test
  public void testMiniClusterTestNode()
  {
    StramMiniClusterTest.TestDNode d = new StramMiniClusterTest.TestDNode();
    d.setNodeContext(new NodeContext("1"));

    d.setTupleCounts("100, 100, 1000");
    Assert.assertEquals("100,100,1000", d.getTupleCounts());

    Assert.assertEquals("heartbeat1", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat2", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat3", 1000, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat4", 100, d.resetHeartbeatCounters().tuplesProcessed);

  }

  @Test
  public void testWindowGen() throws Exception
  {
    final AtomicLong currentWindow = new AtomicLong();
    final AtomicInteger beginWindowCount = new AtomicInteger();
    final AtomicInteger endWindowCount = new AtomicInteger();

    final AtomicLong windowXor = new AtomicLong();

    InputAdapter ia = new InputAdapter()
    {
      @Override
      public void teardown()
      {
      }

      @Override
      public void setup(StreamConfiguration config)
      {
      }

      @Override
      public void endWindow(int windowId)
      {
        endWindowCount.incrementAndGet();
        windowXor.set(windowXor.get() ^ windowId);
        System.out.println("end  : " + windowId + " (" + System.currentTimeMillis() + ")");
      }

      @Override
      public void beginWindow(int windowId)
      {
        currentWindow.set(windowId);
        beginWindowCount.incrementAndGet();
        windowXor.set(windowXor.get() ^ windowId);
        System.out.println("begin: " + windowId + " (" + System.currentTimeMillis() + ")");
      }

      @Override
      public boolean hasFinished()
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public void activate(StreamContext context)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }

      @Override
      public void resetWindow(int baseSeconds, int sizeMillis)
      {
      }
    };

    long startTime = System.currentTimeMillis();
    startTime = startTime - 1000;
    int intervalMillis = 200;
    WindowGenerator wg = new WindowGenerator(Collections.singletonList(ia), startTime, intervalMillis);
    wg.start();

    Thread.sleep(300);

    wg.stop();
    System.out.println("completed windows: " + endWindowCount.get());
    Assert.assertEquals("only last window open", currentWindow.get(), windowXor.get());

    long expectedCnt = (System.currentTimeMillis() - startTime) / intervalMillis;
    Assert.assertEquals("begin window count", expectedCnt + 1, beginWindowCount.get());
    Assert.assertEquals("end window count", expectedCnt, endWindowCount.get());

  }
}
