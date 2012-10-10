/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.stream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.SerDe;
import com.malhartech.dag.Sink;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;

/**
 *
 */
public class SocketStreamTest
{
  private static final Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;

  @BeforeClass
  public static void setup() throws InterruptedException, IOException, Exception
  {
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr = (InetSocketAddress)bufferServer.run();
    bufferServerPort = bindAddr.getPort();
  }

  @AfterClass
  public static void tearDown() throws IOException
  {
    if (bufferServer != null) {
      bufferServer.shutdown();
    }
  }

  /**
   * Test buffer server stream by sending
   * tuple on outputstream and receive same tuple from inputstream
   *
   * @throws Exception
   */
  @Test
  public void testBufferServerStream() throws Exception
  {

    final AtomicInteger messageCount = new AtomicInteger();
    Sink sink = new Sink()
    {
      @Override
      public void process(Object payload)
      {
        if (payload instanceof Tuple) {
          Tuple t = (Tuple)payload;
          switch (t.getType()) {
            case BEGIN_WINDOW:
              break;

            case END_WINDOW:
              synchronized (SocketStreamTest.this) {
                SocketStreamTest.this.notifyAll();
              }
              break;

            default:
          }
        }
        else {
          System.out.println("received: " + payload);
          messageCount.incrementAndGet();
        }
      }
    };

    SerDe serde = new DefaultSerDe();

    String streamName = "streamName"; // AKA "type"
    String upstreamNodeId = "upstreamNodeId";
    String downstreamNodeId = "downStreamNodeId";


    StreamContext issContext = new StreamContext(streamName);
    issContext.setSourceId(upstreamNodeId);
    issContext.setSinkId(downstreamNodeId);

    StreamConfiguration sconf = new StreamConfiguration(Collections.<String, String>emptyMap());
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", bufferServerPort));

    BufferServerInputStream iss = new BufferServerInputStream(serde);
    iss.setup(sconf);
    iss.connect("testSink", sink);

    StreamContext ossContext = new StreamContext(streamName);
    ossContext.setSourceId(upstreamNodeId);
    ossContext.setSinkId(downstreamNodeId);

    BufferServerOutputStream oss = new BufferServerOutputStream(serde);
    oss.setup(sconf);

    iss.activate(issContext);
    LOG.debug("input stream activated");

    oss.activate(ossContext);
    LOG.debug("output stream activated");

    LOG.debug("Sending hello message");
    oss.process(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0));
    oss.process(StramTestSupport.generateTuple("hello", 0));
    oss.process(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0, 1));
    oss.process(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 1)); // it's a spurious tuple, presence of it should not affect the outcome of the test.
    if (messageCount.get() == 0) {
      synchronized (SocketStreamTest.this) {
        SocketStreamTest.this.wait(2000);
      }
    }

    Assert.assertEquals("Received messages", 1, messageCount.get());
  }

}
