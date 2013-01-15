/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.malhartech.bufferserver.BufferServerController;
import com.malhartech.bufferserver.BufferServerPublisher;
import com.malhartech.bufferserver.BufferServerSubscriber;
import com.malhartech.bufferserver.Server;
import com.malhartech.bufferserver.ServerTest.BeginTuple;
import com.malhartech.bufferserver.ServerTest.EndTuple;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
  static Server instance;
  static BufferServerPublisher bsp;
  static BufferServerSubscriber bss;
  static BufferServerController bsc;
  static int spinCount = 500;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    instance = new Server(0, 1024, 10);
    instance.setSpoolStorage(new DiskStorage());

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
  public void testStorage() throws InterruptedException
  {
    bss.activate();

    bsp.baseWindow = 0x7afebabe;
    bsp.windowId = 0;
    bsp.activate();

    BeginTuple bt0 = new BeginTuple();
    bt0.id = 0x7afebabe00000000L;
    bsp.publishMessage(bt0);

    for (int i = 0; i < 1000; i++) {
      bsp.publishMessage(new byte[] {(byte)i});
    }

    EndTuple et0 = new EndTuple();
    et0.id = bt0.id;
    bsp.publishMessage(et0);

    BeginTuple bt1 = new BeginTuple();
    bt1.id = bt0.id + 1;
    bsp.publishMessage(bt1);

    for (int i = 0; i < 1000; i++) {
      bsp.publishMessage(new byte[] {(byte)i});
    }

    EndTuple et1 = new EndTuple();
    et1.id = bt1.id;
    bsp.publishMessage(et1);

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (bss.tupleCount.get() > 2003) {
        break;
      }
    }
    Thread.sleep(10); // wait some more to receive more tuples if possible

    bsp.deactivate();
    bss.deactivate();

    assertEquals(bss.tupleCount.get(), 2004);
  }

}
