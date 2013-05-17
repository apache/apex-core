/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import com.malhartech.bufferserver.server.Server;
import com.malhartech.codec.DefaultStreamCodec;
import com.malhartech.engine.StreamContext;
import com.malhartech.engine.SweepableReservoir;
import com.malhartech.netlet.DefaultEventLoop;
import com.malhartech.netlet.EventLoop;
import com.malhartech.stram.support.StramTestSupport;
import com.malhartech.tuple.EndWindowTuple;
import com.malhartech.tuple.Tuple;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SocketStreamTest
{
  private static final Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;
  static EventLoop eventloop;

  static {
    try {
      eventloop = new DefaultEventLoop("StreamTestEventLoop");
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @BeforeClass
  public static void setup() throws InterruptedException, IOException, Exception
  {
    ((DefaultEventLoop)eventloop).start();
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr = bufferServer.run(eventloop);
    bufferServerPort = bindAddr.getPort();
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (bufferServer != null) {
      eventloop.stop(bufferServer);
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
  @SuppressWarnings({"rawtypes", "unchecked", "SleepWhileInLoop"})
  public void testBufferServerStream() throws Exception
  {
    final StreamCodec<Object> serde = new DefaultStreamCodec<Object>();
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
    issContext.attr(StreamContext.CODEC).set(serde);
    issContext.attr(StreamContext.EVENT_LOOP).set(eventloop);

    BufferServerSubscriber iss = new BufferServerSubscriber(downstreamNodeId, 1024);
    iss.setup(issContext);
    SweepableReservoir reservoir = iss.acquireReservoir("testReservoir", 1);
    reservoir.setSink(sink);

    StreamContext ossContext = new StreamContext(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);
    ossContext.setBufferServerAddress(InetSocketAddress.createUnresolved("localhost", bufferServerPort));
    ossContext.attr(StreamContext.CODEC).set(serde);
    ossContext.attr(StreamContext.EVENT_LOOP).set(eventloop);

    BufferServerPublisher oss = new BufferServerPublisher(upstreamNodeId, 1024);
    oss.setup(ossContext);

    iss.activate(issContext);
    LOG.debug("input stream activated");

    oss.activate(ossContext);
    LOG.debug("output stream activated");

    LOG.debug("Sending hello message");
    oss.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0));
    oss.put(StramTestSupport.generateTuple("hello", 0));
    oss.put(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0));
    oss.put(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1)); // it's a spurious tuple, presence of it should not affect the outcome of the test.

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

  private static final Logger logger = LoggerFactory.getLogger(SocketStreamTest.class);
}
