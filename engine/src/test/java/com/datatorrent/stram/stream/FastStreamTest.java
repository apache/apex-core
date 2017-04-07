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

import org.junit.AfterClass;
import org.junit.Assert;
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
// this is put in ignore test so that it will be moved out of this project into
// bufferserver project and made to work consistently. Currently it has timing
// issues.
@Ignore
public class FastStreamTest
{
  private static final Logger LOG = LoggerFactory.getLogger(FastStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;
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
    final StreamCodec<Object> serde = new DefaultStatefulStreamCodec<>();
    final AtomicInteger messageCount = new AtomicInteger();
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

    String streamName = "streamName";
    String upstreamNodeId = "upstreamNodeId";
    String downstreamNodeId = "downStreamNodeId";

    StreamContext issContext = new StreamContext(streamName);
    issContext.setSourceId(upstreamNodeId);
    issContext.setSinkId(downstreamNodeId);
    issContext.setFinishedWindowId(-1);
    issContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    issContext.put(StreamContext.CODEC, serde);
    issContext.put(StreamContext.EVENT_LOOP, eventloop);

    FastSubscriber subscriber = new FastSubscriber(downstreamNodeId, 1024);
    subscriber.setup(issContext);
    SweepableReservoir reservoir = subscriber.acquireReservoir("testReservoir", 1);
    reservoir.setSink(sink);

    StreamContext ossContext = new StreamContext(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);
    ossContext.setFinishedWindowId(-1);
    ossContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    ossContext.put(StreamContext.CODEC, serde);
    ossContext.put(StreamContext.EVENT_LOOP, eventloop);

    FastPublisher publisher = new FastPublisher(upstreamNodeId, 8);
    StreamContext publisherContext = new StreamContext(streamName);
    publisherContext.setSourceId(upstreamNodeId);
    publisherContext.setSinkId(downstreamNodeId);
    publisherContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    publisherContext.put(StreamContext.CODEC, serde);
    publisherContext.put(StreamContext.EVENT_LOOP, eventloop);
    publisher.setup(publisherContext);

    subscriber.activate(issContext);
    LOG.debug("input stream activated");

    publisher.activate(publisherContext);
    LOG.debug("output stream activated");

    LOG.debug("Sending hello message");
    publisher.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0));
    publisher.put(StramTestSupport.generateTuple("hello", 0));
    publisher.put(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0));
    publisher.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1)); // it's a spurious tuple, presence of it should not affect the outcome of the test.

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

    eventloop.disconnect(publisher);
    eventloop.disconnect(subscriber);
    Assert.assertEquals("Received messages", 1, messageCount.get());
  }

  private static final Logger logger = LoggerFactory.getLogger(FastStreamTest.class);
}
