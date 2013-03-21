/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.client.BufferServerSubscriber;
import com.malhartech.bufferserver.client.BufferServerPublisher;
import com.malhartech.bufferserver.packet.*;
import com.malhartech.bufferserver.server.Server;
import com.malhartech.bufferserver.util.Codec;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicBoolean;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  static String host;
  static int port;
  static DefaultEventLoop eventloopServer;
  static DefaultEventLoop eventloopClient;

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
    SocketAddress result = instance.run(eventloopServer);
    assert (result instanceof InetSocketAddress);
    host = ((InetSocketAddress)result).getHostName();
    port = ((InetSocketAddress)result).getPort();
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
    final BufferServerPublisher bsp1 = new BufferServerPublisher("MyPublisher");
    bsp1.eventloop = eventloopClient;
    bsp1.setup(host, port);

    final BufferServerSubscriber bss1 = new BufferServerSubscriber("MyPublisher", 0, null)
    {
      @Override
      public synchronized void beginWindow(int windowId)
      {
        if (windowId > 9) {
          notifyAll();
        }
      }

    };
    bss1.eventloop = eventloopClient;
    bss1.setup(host, port);

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
        long resetId = 0x7afebabe000000faL;
        bsp1.publishMessage(ResetWindowTuple.getSerializedTuple(resetId));

        long windowId = 0x7afebabe00000000L;
        try {
          while (publisherRun.get()) {
            bsp1.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

            Thread.sleep(5);

            bsp1.publishMessage(PayloadTuple.getSerializedTuple(0, 0));

            Thread.sleep(5);

            bsp1.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            windowId++;
          }
        }
        catch (InterruptedException ex) {
        }
        catch (CancelledKeyException cke) {
        }
        finally {
          logger.debug("publisher the middle of window = {}", Codec.getStringWindowId(windowId));
        }
      }

    }.start();

    synchronized (this) {
      wait(200);
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
    final BufferServerPublisher bsp2 = new BufferServerPublisher("MyPublisher");
    bsp2.eventloop = eventloopClient;
    bsp2.setup(host, port);

    final BufferServerSubscriber bss2 = new BufferServerSubscriber("MyPublisher", 0, null)
    {
      @Override
      public synchronized void beginWindow(int windowId)
      {
        if (windowId > 14) {
          notifyAll();
        }
      }

    };
    bss2.eventloop = eventloopClient;
    bss2.setup(host, port);

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

            Thread.sleep(5);

            byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
            buff[buff.length - 1] = 'a';
            bsp2.publishMessage(buff);

            Thread.sleep(5);

            bsp2.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

            windowId++;
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
      wait(200);
    }

    publisherRun.set(false);

    bsp2.deactivate();
    bss2.deactivate();

    bss2.teardown();
    bsp2.teardown();

    //Assert.assertTrue((bss2.lastPayload.getWindowId() - 8) * 3 < bss2.tupleCount.get());
  }

}
