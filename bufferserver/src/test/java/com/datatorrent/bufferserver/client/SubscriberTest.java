/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.client;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.DefaultEventLoop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class SubscriberTest
{
  private static final Logger logger = LoggerFactory.getLogger(SubscriberTest.class);
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
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp1);

    final Subscriber bss1 = new Subscriber("MySubscriber")
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 9) {
          synchronized (SubscriberTest.this) {
            SubscriberTest.this.notifyAll();
          }
        }
      }

      @Override
      public String toString()
      {
        return "BufferServerSubscriber";
      }

    };
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss1);

    final int baseWindow = 0x7afebabe;
    bsp1.activate(null, baseWindow, 0);
    bss1.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    final AtomicBoolean publisherRun = new AtomicBoolean(true);
    new Thread("publisher")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        bsp1.publishMessage(ResetWindowTuple.getSerializedTuple(baseWindow, 500));

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

    eventloopClient.disconnect(bsp1);
    eventloopClient.disconnect(bss1);

    /*
     * At this point, we know that both the publishers and the subscribers have gotten at least window Id 10.
     * So we go ahead and make the publisher publish from 5 onwards with different data and have subscriber
     * subscribe from 8 onwards. What we should see is that subscriber gets the new data from 8 onwards.
     */
    final Publisher bsp2 = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp2);
    bsp2.activate(null, 0x7afebabe, 5);

    final Subscriber bss2 = new Subscriber("MyPublisher")
    {
      @Override
      public void beginWindow(int windowId)
      {
        super.beginWindow(windowId);
        if (windowId > 14) {
          synchronized (SubscriberTest.this) {
            SubscriberTest.this.notifyAll();
          }
        }
      }

    };
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss2);
    bss2.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0x7afebabe00000008L);


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

    synchronized (this) {
      wait();
    }

    publisherRun.set(false);

    eventloopClient.disconnect(bsp2);
    eventloopClient.disconnect(bss2);

    Assert.assertTrue((bss2.lastPayload.getWindowId() - 8) * 3 < bss2.tupleCount.get());
  }

}
