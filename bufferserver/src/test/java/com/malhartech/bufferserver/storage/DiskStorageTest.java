/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.malhartech.bufferserver.client.BufferServerController;
import com.malhartech.bufferserver.client.BufferServerPublisher;
import com.malhartech.bufferserver.client.BufferServerSubscriber;
import com.malhartech.bufferserver.packet.BeginWindowTuple;
import com.malhartech.bufferserver.packet.EndWindowTuple;
import com.malhartech.bufferserver.packet.PayloadTuple;
import com.malhartech.bufferserver.server.Server;
import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import malhar.netlet.DefaultEventLoop;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class DiskStorageTest
{
  static DefaultEventLoop eventloopServer;
  static DefaultEventLoop eventloopClient;
  static Server instance;
  static BufferServerPublisher bsp;
  static BufferServerSubscriber bss;
  static BufferServerController bsc;
  static int spinCount = 500;

  static String host;
  static int port;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    eventloopServer = new DefaultEventLoop("server");
    eventloopServer.start();

    eventloopClient = new DefaultEventLoop("client");
    eventloopClient.start();

    instance = new Server(0, 1024);
    instance.setSpoolStorage(new DiskStorage());

    SocketAddress result = instance.run(eventloopServer);
    assert (result instanceof InetSocketAddress);
     host = ((InetSocketAddress)result).getHostName();
    port = ((InetSocketAddress)result).getPort();

    bsp = new BufferServerPublisher("MyPublisher");
    bsp.eventloop = eventloopClient;
    bsp.setup(host, port);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bsc = new BufferServerController("MyPublisher");
    bsc.eventloop = eventloopClient;
    bsc.setup(host, port);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    bsc.teardown();
    eventloopServer.stop(instance);
    eventloopServer.stop();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testStorage() throws InterruptedException
  {
    bss.activate();

    bsp.baseWindow = 0x7afebabe;
    bsp.windowId = 0;
    bsp.activate();

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

    bsp.deactivate();
    bss.deactivate();

    bsp.teardown();
    bss.teardown();

    assertEquals(bss.tupleCount.get(), 2004);

    bss = new BufferServerSubscriber("MyPublisher", 0, null);
    bss.eventloop = eventloopClient;
    bss.setup(host, port);

    bss.activate();

    for (int i = 0; i < spinCount; i++) {
      sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible
    bss.deactivate();
    bss.teardown();

    assertEquals(bss.tupleCount.get(), 2004);

  }

}
