/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.storage;

import java.net.InetSocketAddress;
import static java.lang.Thread.sleep;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.support.Controller;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.netlet.DefaultEventLoop;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class DiskStorageTest
{
  static DefaultEventLoop eventloopServer;
  static DefaultEventLoop eventloopClient;
  static Server instance;
  static Publisher bsp;
  static Subscriber bss;
  static Controller bsc;
  static int spinCount = 500;
  static InetSocketAddress address;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    eventloopServer = new DefaultEventLoop("server");
    eventloopServer.start();

    eventloopClient = new DefaultEventLoop("client");
    eventloopClient.start();

    instance = new Server(0, 1024,8);
    instance.setSpoolStorage(new DiskStorage());

    address = instance.run(eventloopServer);
    assert (address instanceof InetSocketAddress);

    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsp);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bsc = new Controller("MyPublisher");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bsc);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    eventloopServer.stop(instance);
    eventloopClient.stop();
    eventloopServer.stop();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testStorage() throws InterruptedException
  {
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    bsp.activate(null, 0x7afebabe, 0);

    long windowId = 0x7afebabe00000000L;
    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 1000; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    windowId++;

    bsp.publishMessage(BeginWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < 1000; i++) {
      byte[] buff = PayloadTuple.getSerializedTuple(0, 1);
      buff[buff.length - 1] = (byte)i;
      bsp.publishMessage(buff);
    }

    bsp.publishMessage(EndWindowTuple.getSerializedTuple((int)windowId));

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    eventloopClient.disconnect(bsp);
    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 2004);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L);

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible
    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 2004);

  }

}
