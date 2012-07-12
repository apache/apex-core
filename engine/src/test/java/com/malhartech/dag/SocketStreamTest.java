/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Server;
import java.io.IOException;
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

  private static Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;

  @BeforeClass
  public static void setup() throws InterruptedException, IOException
  {
    //   java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.FINEST);
    //    java.util.logging.Logger.getLogger("").info("test");
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr = (InetSocketAddress) bufferServer.run();
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
   * Send tuple on outputstream and receive tuple from inputstream
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
      public void doSomething(Tuple t)
      {
        switch (t.getData().getType()) {
          case BEGIN_WINDOW:
            System.out.println(t.getData().getBeginwindow().getNode() + " begin window for window " + t.getData().getWindowId());
            break;

          case END_WINDOW:
            System.out.println(t.getData().getEndwindow().getNode() + " end window for window " + t.getData().getWindowId());
            synchronized (SocketStreamTest.this) {
              SocketStreamTest.this.notifyAll();
            }
            break;

          case SIMPLE_DATA:
            System.out.println("received: " + t.getObject());
            messageCount.incrementAndGet();
        }
      }
    };

    SerDe serde = new DefaultSerDe();


    StreamContext issContext = new StreamContext(sink);
    issContext.setSerde(serde);

    String streamName = "streamName"; // AKA "type"
    String upstreamNodeId = "upstreamNodeId";
    String downstreamNodeId = "downStreamNodeId";

    StreamConfiguration sconf = new StreamConfiguration();
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", bufferServerPort));

    BufferServerInputSocketStream iss = new BufferServerInputSocketStream();
    iss.setup(sconf);
    iss.setContext(issContext, upstreamNodeId, streamName, downstreamNodeId);
    System.out.println("input stream ready");

    BufferServerOutputSocketStream oss = new BufferServerOutputSocketStream();
    StreamContext ossContext = new StreamContext(null);
    ossContext.setSerde(serde);
    oss.setup(sconf);
    oss.setContext(ossContext, upstreamNodeId, streamName);

    LOG.info("Sending hello message");
    oss.doSomething(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0, ossContext));
    oss.doSomething(StramTestSupport.generateTuple("hello", 0, ossContext));
    oss.doSomething(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0, 1, ossContext));
    synchronized (SocketStreamTest.this) {
      if (messageCount.get() == 0) { // don't wait if already notified
        SocketStreamTest.this.wait(2000);
      }
    }

    Assert.assertEquals("Received messages", 1, messageCount.get());
    System.out.println("exiting...");

  }
}
