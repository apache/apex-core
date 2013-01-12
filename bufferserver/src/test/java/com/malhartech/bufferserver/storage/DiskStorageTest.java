/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.storage;

import com.malhartech.bufferserver.BufferServerController;
import com.malhartech.bufferserver.BufferServerPublisher;
import com.malhartech.bufferserver.BufferServerSubscriber;
import com.malhartech.bufferserver.Server;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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
    instance = new Server(0, 4096, 10);

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
  public void testStorage()
  {
  }

}
