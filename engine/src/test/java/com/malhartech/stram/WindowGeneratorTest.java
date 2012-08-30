/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.NodeContext;
import com.malhartech.dag.Sink;
import com.malhartech.dag.Tuple;
import com.malhartech.util.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

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

    Sink s = new Sink()
    {
      @Override
      public void process(Object payload)
      {
        long windowId = ((Tuple)payload).getWindowId();

        switch (((Tuple)payload).getType()) {
          case BEGIN_WINDOW:
            currentWindow.set(windowId);
            beginWindowCount.incrementAndGet();
            windowXor.set(windowXor.get() ^ windowId);
            System.out.println("begin: " + windowId + " (" + System.currentTimeMillis() + ")");
            break;

          case END_WINDOW:
            endWindowCount.incrementAndGet();
            windowXor.set(windowXor.get() ^ windowId);
            System.out.println("end  : " + windowId + " (" + System.currentTimeMillis() + ")");
            break;

          case RESET_WINDOW:
            break;

          default:
            currentWindow.set(0);
            break;
        }
      }
    };

    Configuration config = new Configuration();
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, System.currentTimeMillis() - 1000);
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 200);

    WindowGenerator wg = new WindowGenerator(new ScheduledThreadPoolExecutor(1));
    wg.setup(config);
    wg.connect("GeneratorTester", s);

    wg.activate(null);
    Thread.sleep(300);
    wg.deactivate();

    System.out.println("completed windows: " + endWindowCount.get());
    Assert.assertEquals("only last window open", currentWindow.get(), windowXor.get());

    long expectedCnt = (System.currentTimeMillis() - config.getLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0L))
            / config.getInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 1);

    Assert.assertEquals("begin window count", expectedCnt + 1, beginWindowCount.get());
    Assert.assertEquals("end window count", expectedCnt, endWindowCount.get());
  }
}
