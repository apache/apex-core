/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.*;
import com.malhartech.stram.support.ManualScheduledExecutorService;
import com.malhartech.stram.StramLocalCluster;
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

    generator.setSink(Node.OUTPUT, new Sink<Object>()
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

    generator.activate(null);

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
    ManualScheduledExecutorService msse = new ManualScheduledExecutorService(1);
    msse.setCurrentTimeMillis(0xcafebabe * 1000L);
    WindowGenerator generator = new WindowGenerator(msse);

    final long currentTIme = msse.getCurrentTimeMillis();
    final int windowWidth = 0x1234abcd;
    generator.setFirstWindow(currentTIme);
    generator.setResetWindow(currentTIme);
    generator.setWindowWidth(windowWidth);
    generator.setSink(Node.OUTPUT, new Sink<Object>()
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

    generator.activate(null);
    msse.tick(1);
  }

  @Test
  public void testWindowGen() throws Exception
  {
    final AtomicLong currentWindow = new AtomicLong();
    final AtomicInteger beginWindowCount = new AtomicInteger();
    final AtomicInteger endWindowCount = new AtomicInteger();

    final AtomicLong windowXor = new AtomicLong();

    Sink<Object> s = new Sink<Object>()
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
            break;

          case END_WINDOW:
            endWindowCount.incrementAndGet();
            windowXor.set(windowXor.get() ^ windowId);
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
    firstWindowMillis -= firstWindowMillis % 1000L;

    WindowGenerator wg = new WindowGenerator(new ScheduledThreadPoolExecutor(1, "WindowGenerator"));
    wg.setResetWindow(firstWindowMillis);
    wg.setFirstWindow(firstWindowMillis);
    wg.setWindowWidth(windowWidth);
    wg.setSink("GeneratorTester", s);

    wg.activate(null);
    Thread.sleep(200);
    wg.deactivate();
    long lastWindowMillis = System.currentTimeMillis();

    Assert.assertEquals("only last window open", currentWindow.get(), windowXor.get());

    long expectedCnt = (lastWindowMillis - firstWindowMillis) / windowWidth;

    Assert.assertTrue("Minimum begin window count", expectedCnt + 1 <= beginWindowCount.get());
    Assert.assertEquals("end window count", beginWindowCount.get() - 1, endWindowCount.get());
  }

  static class RandomNumberGenerator implements InputOperator
  {
    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>(this);

    @Override
    public void emitTuples()
    {
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException ex) {
        logger.debug("interrupted!", ex);
      }

      output.emit(++count);
    }

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

    int count;
  }

  static class MyLogger extends BaseOperator
  {
    public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>(this)
    {
      @Override
      public void process(Integer tuple)
      {
        logger.debug("received {}", tuple);
      }

    };
  }

  @Test
  public void testOutofSequenceError() throws Exception
  {
    DAG dag = new DAG(new Configuration());

    RandomNumberGenerator rng = dag.addOperator("random", new RandomNumberGenerator());
    MyLogger ml = dag.addOperator("logger", new MyLogger());

    dag.addStream("stream", rng.output, ml.input);

    StramLocalCluster lc = new StramLocalCluster(dag);
    lc.run(10000);
  }

}
