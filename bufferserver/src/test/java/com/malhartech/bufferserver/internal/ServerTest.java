/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.client.BufferServerController;
import com.malhartech.bufferserver.client.BufferServerPublisher;
import com.malhartech.bufferserver.client.BufferServerSubscriber;
import com.malhartech.bufferserver.packet.BeginWindowTuple;
import com.malhartech.bufferserver.packet.EndWindowTuple;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.bufferserver.packet.ResetWindowTuple;
import com.malhartech.bufferserver.server.Server;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ServerTest
{
  private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
  static Server instance;
  static String host;
  static int port;
  static BufferServerPublisher bsp;
  static BufferServerSubscriber bss;
  static BufferServerController bsc;
  static int spinCount = 300;
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

    instance = new Server(0, 4096);
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
  }

  @Test
  public void testNoPublishNoSubscribe() throws InterruptedException
  {
    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bsp.activate();
    bss.activate();

    synchronized (this) {
      wait(100);
    }

    bss.deactivate();
    bsp.deactivate();

    bss.teardown();
    bsp.teardown();
    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testNoPublishNoSubscribe"})
  @SuppressWarnings("SleepWhileInLoop")
  public void test1Window() throws InterruptedException
  {
    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bsp.activate();
    bss.activate();

    long resetInfo = 0x7afebabe000000faL;

    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);

    bss.deactivate();
    bsp.deactivate();

    bss.teardown();
    bsp.teardown();
    assertEquals(bss.tupleCount.get(), 1);
    Assert.assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"test1Window"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testLateSubscriber() throws InterruptedException
  {
    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.activate();

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);

    bss.deactivate();
    bss.teardown();

    assertEquals(bss.tupleCount.get(), 1);
    Assert.assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"testLateSubscriber"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testATonOfData() throws InterruptedException
  {
    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);
    bss.activate();

    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);
    bsp.baseWindow = 0x7afebabe;
    bsp.windowId = 0;
    bsp.activate();

    long windowId = 0x7afebabe00000000L;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 100; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;


    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 100; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 204) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    bsp.deactivate();
    bss.deactivate();

    bsp.teardown();
    bss.teardown();
    assertEquals(bss.tupleCount.get(), 205);
  }

  @Test(dependsOnMethods = {"testATonOfData"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeNonExistent() throws InterruptedException
  {

    bsc = new BufferServerController("MyPublisher");
    bsc.eventloop = eventloopClient;
    bsc.setup(host, port);

    bsc.windowId = 0;
    bsc.activate();
    bsc.purge();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.deactivate();
    bsc.teardown();

    assertNotNull(bsc.data);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);
    bss.activate();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 204) {
        break;
      }
    }
    Thread.sleep(10);
    bss.deactivate();
    bss.teardown();
    assertEquals(bss.tupleCount.get(), 205);
  }

  @Test(dependsOnMethods = {"testPurgeNonExistent"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeSome() throws InterruptedException
  {
    bsc = new BufferServerController("MyPublisher");
    bsc.eventloop = eventloopClient;
    bsc.setup(host, port);

    bsc.windowId = 0x7afebabe00000000L;
    bsc.activate();
    bsc.purge();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.deactivate();
    bsc.teardown();

    assertNotNull(bsc.data);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);
    bss.activate();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 102) {
        break;
      }
    }
    bss.deactivate();
    bss.teardown();
    assertEquals(bss.tupleCount.get(), 103);
  }

  @Test(dependsOnMethods = {"testPurgeSome"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeAll() throws InterruptedException
  {
    bsc = new BufferServerController("MyPublisher");
    bsc.eventloop = eventloopClient;
    bsc.setup(host, port);

    bsc.windowId = 0x7afebabe00000001L;
    bsc.activate();
    bsc.purge();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.deactivate();
    bsc.teardown();

    assertNotNull(bsc.data);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.activate();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);
    bss.deactivate();
    bss.teardown();
    assertEquals(bss.tupleCount.get(), 1);
  }

  @Test(dependsOnMethods = {"testPurgeAll"})
  public void testRepublish() throws InterruptedException
  {
    testATonOfData();
  }

  @Test(dependsOnMethods = {"testRepublish"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testReblishLowerWindow() throws InterruptedException
  {
    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);

    bsp.baseWindow = 10;
    bsp.windowId = 0;
    bsp.activate();

    long windowId = 0L;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 2; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 2; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    bsp.deactivate();
    bsp.teardown();

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.activate();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 7) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    bss.deactivate();
    bss.teardown();

    assertEquals(bss.tupleCount.get(), 8);
  }

  @Test(dependsOnMethods = {"testReblishLowerWindow"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testReset() throws InterruptedException
  {
    bsc = new BufferServerController("MyPublisher");
    bsc.eventloop = eventloopClient;
    bsc.setup(host, port);

    bsc.windowId = 0x7afebabe00000001L;
    bsc.activate();
    bsc.reset();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    bsc.deactivate();
    bsc.teardown();

    assertNotNull(bsc.data);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.activate();
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 0) {
        break;
      }
    }

    bss.deactivate();
    bss.teardown();

    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testReset"})
  public void test1WindowAgain() throws InterruptedException
  {
    test1Window();
  }

  @Test(dependsOnMethods = {"test1WindowAgain"})
  public void testResetAgain() throws InterruptedException
  {
    testReset();
  }

  @Test(dependsOnMethods = {"testResetAgain"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testEarlySubscriberForLaterWindow() throws InterruptedException
  {
    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.windowId = 50;
    bss.activate();

    /* wait in a hope that the subscriber is able to reach the server */
    Thread.sleep(100);
    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);


    bsp.baseWindow = 0;
    bsp.windowId = 0;
    bsp.activate();

    for (int i = 0; i < 100; i++) {
      bsp.publishMessage(BeginWindowTuple.getSerializedTuple(i));

      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);

      bsp.publishMessage(EndWindowTuple.getSerializedTuple(i));
    }

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 149) {
        break;
      }
    }

    Thread.sleep(10);

    bsp.deactivate();
    bsp.teardown();

    assertEquals(bss.tupleCount.get(), 150);

    bss.deactivate();
    bss.teardown();
  }

}
