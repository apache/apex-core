/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.client.Publisher;
import com.malhartech.bufferserver.client.Subscriber;
import com.malhartech.bufferserver.packet.BeginWindowTuple;
import com.malhartech.bufferserver.packet.EndWindowTuple;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.bufferserver.packet.ResetWindowTuple;
import com.malhartech.bufferserver.server.Server;
import com.malhartech.bufferserver.util.Codec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicBoolean;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
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
  static DefaultEventLoop eventloopServer;
  static DefaultEventLoop eventloopClient;
  static InetSocketAddress address;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    try {
      eventloopServer = new DefaultEventLoop("server");
      eventloopClient = new DefaultEventLoop("client");
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    eventloopServer.start();
    eventloopClient.start();

    instance = new Server(0);
    address = instance.run(eventloopServer);
    assert (address instanceof InetSocketAddress);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    eventloopServer.stop(instance);
    eventloopServer.stop();
    eventloopClient.stop();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void test() throws InterruptedException
  {
    final Publisher bsp1 = new Publisher("MyPublisher");
    bsp1.setup(address, eventloopClient);

    final Subscriber bss1 = new Subscriber("MyPublisher", 0, null)
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 9) {
          synchronized (NewSubscriberTest.this) {
            NewSubscriberTest.this.notifyAll();
          }
        }
      }

      @Override
      public String toString()
      {
        return "BufferServerSubscriber";
      }

    };
    bss1.setup(address, eventloopClient);

    bsp1.baseWindow = 0x7afebabe;
    bsp1.windowId = 00000000;
    bsp1.activate();
    bss1.activate();

    final AtomicBoolean publisherRun = new AtomicBoolean(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        bsp1.publishMessage(ResetWindowTuple.getSerializedTuple(bsp1.baseWindow, 500));

        long windowId = 0x7afebabe00000000L;
        try {
          while (publisherRun.get()) {
            bsp1.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

            bsp1.publishMessage(PayloadTuple.getSerializedTuple(0, 0));

            bsp1.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            windowId++;
            Thread.sleep(5);
          }
        }
        catch (InterruptedException ex) {
        }
        catch (CancelledKeyException cke) {
          logger.debug("exception", cke);
        }
        finally {
          logger.debug("publisher the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }

    }.start();

    synchronized (this) {
      wait();
    }

    publisherRun.set(false);

    bsp1.deactivate();
    bss1.deactivate();

    bss1.teardown();
    bsp1.teardown();

    /*
     * At this point, we know that both the publishers and the subscribers have gotten at least window Id 10.
     * So we go ahead and make the publisher publish from 5 onwards with different data and have subscriber
     * subscribe from 8 onwards. What we should see is that subscriber gets the new data from 8 onwards.
     */
    final Publisher bsp2 = new Publisher("MyPublisher");
    bsp2.setup(address, eventloopClient);

    final Subscriber bss2 = new Subscriber("MyPublisher", 0, null)
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 14) {
          synchronized (NewSubscriberTest.this) {
            NewSubscriberTest.this.notifyAll();
          }
        }
      }

    };
    bss2.setup(address, eventloopClient);

    bsp2.baseWindow = 0x7afebabe;
    bsp2.windowId = 5;
    bsp2.activate();

    publisherRun.set(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        long windowId = 0x7afebabe00000005L;
        try {
          while (publisherRun.get()) {
            bsp2.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

            byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
            buff[buff.length - 1] = 'a';
            bsp2.publishMessage(buff);

            bsp2.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            windowId++;
            Thread.sleep(5);
          }
        }
        catch (InterruptedException ex) {
        }
        catch (CancelledKeyException cke) {
        }
        finally {
          logger.debug("publisher in the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }

    }.start();

    bss2.windowId = 0x7afebabe00000008L;
    bss2.activate();

    synchronized (this) {
      wait();
    }

    publisherRun.set(false);

    bsp2.deactivate();
    bss2.deactivate();

    bss2.teardown();
    bsp2.teardown();

    Assert.assertTrue((bss2.lastPayload.getWindowId() - 8) * 3 < bss2.tupleCount.get());
  }

}
