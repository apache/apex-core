/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.dag.WindowGenerator;
import com.malhartech.api.Sink;
import com.malhartech.dag.*;
import com.malhartech.util.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowGeneratorTest
{
  public static final Logger logger = LoggerFactory.getLogger(WindowGeneratorTest.class);

  @Test
  public void test2ndResetWindow() throws InterruptedException
  {
    logger.debug("Testing 2nd Reset Window");

    ManualScheduledExecutorService msse = new ManualScheduledExecutorService(1);
    WindowGenerator generator = new WindowGenerator(msse);

    generator.setFirstWindow(0L);
    generator.setResetWindow(0L);
    generator.setWindowWidth(1);

    final AtomicInteger beginWindowCount = new AtomicInteger(0);
    final AtomicInteger endWindowCount = new AtomicInteger(0);
    final AtomicInteger resetWindowCount = new AtomicInteger(0);
    final AtomicBoolean loggingEnabled = new AtomicBoolean(true);

    generator.setSink(Node.OUTPUT, new Sink()
    {
      @Override
      public void process(Object payload)
      {
        if (loggingEnabled.get()) {
          logger.debug(payload.toString());
        }

        switch (((Tuple)payload).getType()) {
          case BEGIN_WINDOW:
            beginWindowCount.incrementAndGet();
            break;

          case END_WINDOW:
            endWindowCount.incrementAndGet();
            break;

          case RESET_WINDOW:
            resetWindowCount.incrementAndGet();
            break;
        }
      }
    });

    generator.postActivate(null);

    msse.tick(1);
    msse.tick(1);
    loggingEnabled.set(false);
    for (int i = 0; i < WindowGenerator.MAX_WINDOW_ID - 2; i++) {
      msse.tick(1);
    }
    loggingEnabled.set(true);
    msse.tick(1);

    Thread.sleep(20);

    Assert.assertEquals("begin windows", WindowGenerator.MAX_WINDOW_ID + 1 + 1, beginWindowCount.get());
    Assert.assertEquals("end windows", WindowGenerator.MAX_WINDOW_ID + 1, endWindowCount.get());
    Assert.assertEquals("reset windows", 2, resetWindowCount.get());
  }

  /**
   * Test of resetWindow functionality of WindowGenerator.
   */
  @Test
  public void testResetWindow()
  {
    System.out.println("resetWindow");

    ManualScheduledExecutorService msse = new ManualScheduledExecutorService(1);
    msse.setCurrentTimeMillis(0xcafebabe * 1000L);
    WindowGenerator generator = new WindowGenerator(msse);

    final long currentTIme = msse.getCurrentTimeMillis();
    final int windowWidth = 0x1234abcd;
    generator.setFirstWindow(currentTIme);
    generator.setResetWindow(currentTIme);
    generator.setWindowWidth(windowWidth);
    generator.setSink(Node.OUTPUT, new Sink()
    {
      boolean firsttime = true;

      @Override
      public void process(Object payload)
      {
        if (firsttime) {
          assert (payload instanceof ResetWindowTuple);
          assert (((ResetWindowTuple)payload).getWindowId() == 0xcafebabe00000000L);
          assert (((ResetWindowTuple)payload).getBaseSeconds() * 1000L == currentTIme);
          assert (((ResetWindowTuple)payload).getIntervalMillis() == windowWidth);
          firsttime = false;
        }
        else {
          assert (payload instanceof Tuple);
          assert (((Tuple)payload).getWindowId() == 0xcafebabe00000000L);
        }
      }
    });

    generator.postActivate(null);
    msse.tick(1);
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
            System.out.println("begin: " + Long.toHexString(windowId) + " (" + Long.toHexString(System.currentTimeMillis() / 1000) + ")");
            break;

          case END_WINDOW:
            endWindowCount.incrementAndGet();
            windowXor.set(windowXor.get() ^ windowId);
            System.out.println("end  : " + Long.toHexString(windowId) + " (" + Long.toHexString(System.currentTimeMillis() / 1000) + ")");
            break;

          case RESET_WINDOW:
            break;

          default:
            currentWindow.set(0);
            break;
        }
      }
    };

    ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1, "WindowGenerator");
    int windowWidth = 200;
    long firstWindowMillis = stpe.getCurrentTimeMillis();
    firstWindowMillis = firstWindowMillis - firstWindowMillis % 1000L;

    WindowGenerator wg = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"));
    wg.setResetWindow(firstWindowMillis);
    wg.setFirstWindow(firstWindowMillis);
    wg.setWindowWidth(windowWidth);
    wg.setSink("GeneratorTester", s);

    wg.postActivate(null);
    Thread.sleep(200);
    wg.preDeactivate();
    long lastWindowMillis = System.currentTimeMillis();


    System.out.println("firstWindowMillis: " + firstWindowMillis + " lastWindowMillis: " + lastWindowMillis + " completed windows: " + endWindowCount.get());
    Assert.assertEquals("only last window open", currentWindow.get(), windowXor.get());

    long expectedCnt = (lastWindowMillis - firstWindowMillis) / windowWidth;

    Assert.assertTrue("Minimum begin window count", expectedCnt + 1 <= beginWindowCount.get());
    Assert.assertEquals("end window count", beginWindowCount.get() - 1, endWindowCount.get());
  }
}
