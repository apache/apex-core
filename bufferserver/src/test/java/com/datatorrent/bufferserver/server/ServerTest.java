/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.malhartech.bufferserver.packet.BeginWindowTuple;
import com.malhartech.bufferserver.packet.EndWindowTuple;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.bufferserver.packet.ResetWindowTuple;
import com.malhartech.bufferserver.support.Controller;
import com.malhartech.bufferserver.support.Publisher;
import com.malhartech.bufferserver.support.Subscriber;
import com.malhartech.netlet.DefaultEventLoop;
import java.io.IOException;
import java.net.InetSocketAddress;
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
  static Server instance;
  static InetSocketAddress address;
  static Publisher bsp;
  static Subscriber bss;
  static Controller bsc;
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
    address = instance.run(eventloopServer);
    assert (address instanceof InetSocketAddress);
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
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    synchronized (this) {
      wait(100);
    }

    eventloopClient.disconnect(bss);
    eventloopClient.disconnect(bsp);

    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testNoPublishNoSubscribe"})
  @SuppressWarnings("SleepWhileInLoop")
  public void test1Window() throws InterruptedException
  {
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    long resetInfo = 0x7afebabe000000faL;

    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);

    eventloopClient.disconnect(bss);
    eventloopClient.disconnect(bsp);

    assertEquals(bss.tupleCount.get(), 1);
    Assert.assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"test1Window"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testLateSubscriber() throws InterruptedException
  {
    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);

    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 1);
    Assert.assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"testLateSubscriber"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testATonOfData() throws InterruptedException
  {
    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);
    bsp.activate(null, 0x7afebabe, 0);

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
      if (bss.tupleCount.get() == 204 + bss.resetPayloads.size()) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    eventloopClient.disconnect(bsp);
    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 204 + bss.resetPayloads.size());
  }

  @Test(dependsOnMethods = {"testATonOfData"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeNonExistent() throws InterruptedException
  {

    bsc = new Controller("MyController");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);

    bsc.purge(null, "MyPublisher", 0);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    eventloopClient.disconnect(bsc);

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() == 205) {
        break;
      }
    }
    Thread.sleep(10);
    eventloopClient.disconnect(bss);
    assertEquals(bss.tupleCount.get(), 205);
  }

  @Test(dependsOnMethods = {"testPurgeNonExistent"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeSome() throws InterruptedException
  {
    bsc = new Controller("MyController");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);

    bsc.purge(null, "MyPublisher", 0x7afebabe00000000L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    eventloopClient.disconnect(bsc);

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() == 103) {
        break;
      }
    }
    eventloopClient.disconnect(bss);
    assertEquals(bss.tupleCount.get(), 103);
  }

  @Test(dependsOnMethods = {"testPurgeSome"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testPurgeAll() throws InterruptedException
  {
    bsc = new Controller("MyController");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);

    bsc.purge(null, "MyPublisher", 0x7afebabe00000001L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    eventloopClient.disconnect(bsc);

    assertNotNull(bsc.data);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);
    eventloopClient.disconnect(bss);
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
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);

    bsp.activate(null, 10, 0);

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

    eventloopClient.disconnect(bsp);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() == 8) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 8);
  }

  @Test(dependsOnMethods = {"testReblishLowerWindow"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testReset() throws InterruptedException
  {
    bsc = new Controller("MyController");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);

    bsc.reset(null, "MyPublisher", 0x7afebabe00000001L);
    for (int i = 0; i < spinCount * 2; i++) {
      Thread.sleep(10);
      if (bsc.data != null) {
        break;
      }
    }
    eventloopClient.disconnect(bsc);

    assertNotNull(bsc.data);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);
    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 0) {
        break;
      }
    }

    eventloopClient.disconnect(bss);

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
    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 50L);

    /* wait in a hope that the subscriber is able to reach the server */
    Thread.sleep(100);
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);


    bsp.activate(null, 0, 0);

    for (int i = 0; i < 100; i++) {
      bsp.publishMessage(BeginWindowTuple.getSerializedTuple(i));

      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);

      bsp.publishMessage(EndWindowTuple.getSerializedTuple(i));
    }

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() == 150) {
        break;
      }
    }

    Thread.sleep(10);

    eventloopClient.disconnect(bsp);

    assertEquals(bss.tupleCount.get(), 150);

    eventloopClient.disconnect(bss);
  }

  private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
}
