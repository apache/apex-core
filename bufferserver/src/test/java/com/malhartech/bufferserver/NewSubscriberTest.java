/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.util.Codec;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class NewSubscriberTest
{
  private static final Logger logger = LoggerFactory.getLogger(NewSubscriberTest.class);
  static Server instance;
  static BufferServerPublisher bsp;
  static BufferServerSubscriber bss;
  static BufferServerController bsc;
  static int spinCount = 500;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    instance = new Server(0);
    SocketAddress result = instance.run();
    assert (result instanceof InetSocketAddress);
    String host = ((InetSocketAddress)result).getHostName();
    int port = ((InetSocketAddress)result).getPort();

    bsp = new BufferServerPublisher("MyPublisher");
    bsp.setup(host, port);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.setup(host, port);

    bsc = new BufferServerController("MyPublisher");
    bsc.setup(host, port);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    bsc.teardown();
    bss.teardown();
    bsp.teardown();
    instance.shutdown();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void test() throws InterruptedException
  {
    bsp.baseWindow = 0x7afebabe;
    bsp.windowId = 00000000;
    bsp.activate();
    bss.activate();

    Thread.sleep(500);

    final AtomicBoolean publisherRun = new AtomicBoolean(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        ResetTuple rt = new ResetTuple();
        rt.id = 0x7afebabe000000faL;
        bsp.publishMessage(rt);

        long windowId = 0x7afebabe00000000L;
        try {
          while (publisherRun.get()) {

            BeginTuple bt = new BeginTuple();
            bt.id = windowId;
            bsp.publishMessage(bt);

            Thread.sleep(5);
            bsp.publishMessage(new byte[0]);
            Thread.sleep(5);

            EndTuple et = new EndTuple();
            et.id = windowId;
            bsp.publishMessage(et);

            windowId++;
          }
        }
        catch (InterruptedException ex) {
        }
        finally {
          logger.debug("publisher the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }
    }.start();


    final AtomicBoolean subscriberRun = new AtomicBoolean(true);
    new Thread("subscriber")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        try {
          while (subscriberRun.get()) {
            Thread.sleep(10);
//            logger.debug("subscriber received first = {} and last = {}", bss.firstPayload, bss.lastPayload);

          }
        }
        catch (InterruptedException ex) {
        }
        finally {
          logger.debug("subscriber received first = {} and last = {}", bss.firstPayload, bss.lastPayload);
        }
      }
    }.start();

    while (bss.lastPayload == null || bss.lastPayload.getWindowId() < 10) {
      Thread.sleep(10);
    }

    publisherRun.set(false);
    subscriberRun.set(false);

    bsp.deactivate();
    bss.deactivate();

    /*
     * At this point, we know that both the publishers and the subscribers have gotten at least window Id 10.
     * So we go ahead and make the publisher publish from 5 onwards with different data and have subscriber
     * subscribe from 8 onwards. What we should see is that subscriber gets the new data from 8 onwards.
     */

    bsp.windowId = 5;
    bsp.activate();
    Thread.sleep(500);

    publisherRun.set(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
//        ResetTuple rt = new ResetTuple();
//        rt.id = 0x7afebabe000000faL;
//        bsp.publishMessage(rt);

        long windowId = 0x7afebabe00000005L;
        try {
          while (publisherRun.get()) {
            BeginTuple bt = new BeginTuple();
            bt.id = windowId;
            bsp.publishMessage(bt);

            Thread.sleep(5);
            bsp.publishMessage(new byte[]{'a'});
            Thread.sleep(5);

            EndTuple et = new EndTuple();
            et.id = windowId;
            bsp.publishMessage(et);

            windowId++;
          }
        }
        catch (InterruptedException ex) {
        }
        finally {
          logger.debug("publisher the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }
    }.start();

    bss.windowId = 0x7afebabe00000008L;
    bss.activate();
    subscriberRun.set(true);

    new Thread("subscriber")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        try {
          while (subscriberRun.get()) {
            Thread.sleep(10);
          }
        }
        catch (InterruptedException ex) {
        }
        finally {
          logger.debug("subscriber received first = {} and last = {}", bss.firstPayload, bss.lastPayload);
        }
      }
    }.start();

    while (bss.lastPayload == null || bss.lastPayload.getWindowId() < 15) {
      Thread.sleep(10);
    }

    publisherRun.set(false);
    subscriberRun.set(false);

    bsp.deactivate();
    bss.deactivate();


    Assert.assertTrue((bss.lastPayload.getWindowId() - 8) * 3 <  bss.tupleCount.get());
  }

  class ResetTuple implements Tuple
  {
    long id;

    public DataType getType()
    {
      return DataType.RESET_WINDOW;
    }

    public long getWindowId()
    {
      return id;
    }

    public int getIntervalMillis()
    {
      return (int)id;
    }

    public int getBaseSeconds()
    {
      return (int)(id >> 32);
    }
  }

  class BeginTuple extends ResetTuple
  {
    @Override
    public DataType getType()
    {
      return DataType.BEGIN_WINDOW;
    }
  }

  class EndTuple extends ResetTuple
  {
    @Override
    public DataType getType()
    {
      return DataType.END_WINDOW;
    }
  }
}
