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
package com.datatorrent.stram.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.netlet.DefaultEventLoop;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.stram.codec.DefaultStatefulStreamCodec;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.tuple.EndWindowTuple;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 *
 */
@Ignore // ignored since they do not belong here!
public class SocketStreamTest
{
  private static final Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;

  private static final String streamName = "streamName";
  private static final String upstreamNodeId = "upstreamNodeId";
  private static final String  downstreamNodeId = "downStreamNodeId";

  private StreamContext issContext;
  private StreamContext ossContext;
  private SweepableReservoir reservoir;
  private BufferServerSubscriber iss;
  private BufferServerPublisher oss;
  private AtomicInteger messageCount;

  static EventLoop eventloop;

  static {
    try {
      eventloop = DefaultEventLoop.createEventLoop("StreamTestEventLoop");
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @BeforeClass
  public static void setup() throws InterruptedException, IOException, Exception
  {
    ((DefaultEventLoop)eventloop).start();
    bufferServer = new Server(eventloop, 0); // find random port
    InetSocketAddress bindAddr = bufferServer.run();
    bufferServerPort = bindAddr.getPort();
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (bufferServer != null) {
      bufferServer.stop();
    }
    ((DefaultEventLoop)eventloop).stop();
  }

  /**
   * Test buffer server stream by sending
   * tuple on outputstream and receive same tuple from inputstream
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop"})
  public void testBufferServerStream() throws Exception
  {
    iss.activate(issContext);
    LOG.debug("input stream activated");

    oss.activate(ossContext);
    LOG.debug("output stream activated");

    sendMessage();
  }

  /**
   * Test buffer server stream by sending
   * tuple on outputstream and receive same tuple from inputstream with following changes
   *
   * 1. Sink is sweeped befere the BufferServerSubscriber is activated.
   * 2. BufferServerSubscriber is activated after the messages are sent from BufferServerPublisher
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop"})
  public void testBufferServerStreamWithLateActivationForSubscriber() throws Exception
  {
    for (int i = 0; i < 50; i++) {
      Tuple t = reservoir.sweep();
      if (t == null) {
        sleep(5);
        continue;
      }

      throw new Exception("Unexpected control tuple.");
    }

    oss.activate(ossContext);
    LOG.debug("output stream activated");

    sendMessage();

    iss.activate(issContext);
    LOG.debug("input stream activated");
  }

  @Before
  public void init()
  {
    final StreamCodec<Object> serde = new DefaultStatefulStreamCodec<>();
    messageCount = new AtomicInteger(0);

    Sink<Object> sink = new Sink<Object>()
    {
      @Override
      public void put(Object tuple)
      {
        logger.debug("received: " + tuple);
        messageCount.incrementAndGet();
      }

      @Override
      public int getCount(boolean reset)
      {
        throw new UnsupportedOperationException("Not supported yet.");
      }
    };

    issContext = new StreamContext(streamName);
    issContext.setSourceId(upstreamNodeId);
    issContext.setSinkId(downstreamNodeId);
    issContext.setFinishedWindowId(-1);
    issContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    issContext.put(StreamContext.CODEC, serde);
    issContext.put(StreamContext.EVENT_LOOP, eventloop);

    iss = new BufferServerSubscriber(downstreamNodeId, 1024);
    iss.setup(issContext);
    reservoir = iss.acquireReservoir("testReservoir", 1);
    reservoir.setSink(sink);

    ossContext = new StreamContext(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);
    ossContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    ossContext.put(StreamContext.CODEC, serde);
    ossContext.put(StreamContext.EVENT_LOOP, eventloop);

    oss = new BufferServerPublisher(upstreamNodeId, 1024);
    oss.setup(ossContext);
  }

  @After
  public void verify() throws InterruptedException
  {
    for (int i = 0; i < 100; i++) {
      Tuple t = reservoir.sweep();
      if (t == null) {
        sleep(5);
        continue;
      }

      reservoir.remove();
      if (t instanceof EndWindowTuple) {
        break;
      }
    }

    eventloop.disconnect(oss);
    eventloop.disconnect(iss);
    Assert.assertEquals("Received messages", 1, messageCount.get());
  }

  private void sendMessage()
  {
    LOG.debug("Sending hello message");
    oss.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0));
    oss.put(StramTestSupport.generateTuple("hello", 0));
    oss.put(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0));
    oss.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1)); // it's a spurious tuple, presence of it should not affect the outcome of the test.
  }

  private static final Logger logger = LoggerFactory.getLogger(SocketStreamTest.class);
}
