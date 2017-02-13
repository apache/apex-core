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
package com.datatorrent.bufferserver.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datatorrent.bufferserver.packet.BeginWindowTuple;
import com.datatorrent.bufferserver.packet.EndWindowTuple;
import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.bufferserver.packet.ResetWindowTuple;
import com.datatorrent.bufferserver.support.Controller;
import com.datatorrent.bufferserver.support.Publisher;
import com.datatorrent.bufferserver.support.Subscriber;
import com.datatorrent.netlet.DefaultEventLoop;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 *
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

  static byte[] authToken;

  @BeforeClass
  public static void setupServerAndClients() throws Exception
  {
    try {
      eventloopServer = DefaultEventLoop.createEventLoop("server");
      eventloopClient = DefaultEventLoop.createEventLoop("client");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    eventloopServer.start();
    eventloopClient.start();

    instance = new Server(eventloopServer, 0, 4096,8);
    address = instance.run();
    assertTrue(address instanceof InetSocketAddress);
    assertFalse(address.isUnresolved());

    SecureRandom random = new SecureRandom();
    authToken = new byte[20];
    random.nextBytes(authToken);
  }

  @AfterClass
  public static void teardownServerAndClients()
  {
    instance.stop();
    eventloopServer.stop();
  }

  @Test
  public void testNoPublishNoSubscribe() throws InterruptedException
  {
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);

    bss = new Subscriber("MySubscriber");
    eventloopClient.connect(address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    synchronized (this) {
      wait(100);
    }

    eventloopClient.disconnect(bss);
    eventloopClient.disconnect(bsp);

    assertEquals(bss.tupleCount.get(), 0);
  }

  @Test(dependsOnMethods = {"testNoPublishNoSubscribe"}, timeOut = 50)
  @SuppressWarnings("SleepWhileInLoop")
  public void test1Window() throws InterruptedException
  {
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);

    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    long resetInfo = 0x7afebabe000000faL;

    bsp.publishMessage(ResetWindowTuple.getSerializedTuple((int)(resetInfo >> 32), 500));

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }

    while (bss.tupleCount.get() != 1) {
      Thread.sleep(10);
    }

    eventloopClient.disconnect(bss);
    eventloopClient.disconnect(bsp);

    assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"test1Window"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testLateSubscriber() throws InterruptedException
  {
    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    for (int i = 0; i < spinCount; i++) {
      Thread.sleep(10);
      if (!bss.resetPayloads.isEmpty()) {
        break;
      }
    }
    Thread.sleep(10);

    eventloopClient.disconnect(bss);

    assertEquals(bss.tupleCount.get(), 1);
    assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"testLateSubscriber"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testATonOfData() throws InterruptedException
  {
    bss = new Subscriber("MyPublisher");
    eventloopClient.connect(address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);
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
    eventloopClient.connect(address, bsc);

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
    eventloopClient.connect(address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
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
    eventloopClient.connect(address, bsc);

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
    eventloopClient.connect(address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
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
    eventloopClient.connect(address, bsc);

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
    eventloopClient.connect(address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
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
  public void testRepublishLowerWindow() throws InterruptedException
  {
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);

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
    eventloopClient.connect(address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
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

  @Test(dependsOnMethods = {"testRepublishLowerWindow"})
  @SuppressWarnings("SleepWhileInLoop")
  public void testReset() throws InterruptedException
  {
    bsc = new Controller("MyController");
    eventloopClient.connect(address, bsc);

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
    eventloopClient.connect(address, bss);

    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);
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
    eventloopClient.connect(address, bss);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 49L, 0);

    /* wait in a hope that the subscriber is able to reach the server */
    Thread.sleep(100);
    bsp = new Publisher("MyPublisher");
    eventloopClient.connect(address, bsp);


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

  @Test(dependsOnMethods = {"testEarlySubscriberForLaterWindow"})
  public void testAuth() throws InterruptedException
  {
    instance.setAuthToken(authToken);

    bsp = new Publisher("MyPublisher");
    bsp.setToken(authToken);
    eventloopClient.connect(address, bsp);

    bss = new Subscriber("MySubscriber");
    bss.setToken(authToken);
    eventloopClient.connect(address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

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
    assertFalse(bss.resetPayloads.isEmpty());
  }

  @Test(dependsOnMethods = {"testAuth"})
  public void testAuthFailure() throws InterruptedException
  {
    byte[] authToken = ServerTest.authToken.clone();
    authToken[0] = (byte)(authToken[0] + 1);

    bsp = new Publisher("MyPublisher");
    bsp.setToken(authToken);
    eventloopClient.connect(address, bsp);

    bss = new Subscriber("MySubscriber");
    bss.setToken(authToken);
    eventloopClient.connect(address, bss);

    bsp.activate(null, 0L);
    bss.activate(null, "BufferServerOutput/BufferServerSubscriber", "MyPublisher", 0, null, 0L, 0);

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

    assertEquals(bss.tupleCount.get(), 0);
    assertTrue(bss.resetPayloads.isEmpty());
  }

  private static final Logger logger = LoggerFactory.getLogger(ServerTest.class);
}
