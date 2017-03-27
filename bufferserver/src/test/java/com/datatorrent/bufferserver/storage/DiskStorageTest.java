/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.storage;

import java.net.InetSocketAddress;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.bufferserver.support.Controller;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.netlet.DefaultEventLoop;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 *
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
    eventloopServer = DefaultEventLoop.createEventLoop("server");
    eventloopServer.start();

    eventloopClient = DefaultEventLoop.createEventLoop("client");
    eventloopClient.start();

    instance = new Server(eventloopServer, 0, 1024, 8);
    instance.setSpoolStorage(new DiskStorage());

    address = instance.run();
    assertFalse(address.isUnresolved());

    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address, bss);

    bsc = new Controller("MyPublisher");
    eventloopClient.connect(address, bsc);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    instance.stop();
    eventloopClient.stop();
    eventloopServer.stop();
  }

  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testStorage() throws InterruptedException
  {
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

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
    eventloopClient.connect(address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

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
