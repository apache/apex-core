/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.StramTestSupport.MySerDe;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SocketStreamTest
{

  private static Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);

  static {
    //   java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.FINEST);
    //    java.util.logging.Logger.getLogger("").info("test");
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
            break;

          case SIMPLE_DATA:
            System.out.println("received: " + t.getObject());
            synchronized (SocketStreamTest.this) {
              messageCount.incrementAndGet();
            }
        }
      }
    };

    SerDe serde = new MySerDe();

    int port = 0; // find random port
    com.malhartech.bufferserver.Server s = new Server(port);
    InetSocketAddress bindAddr = (InetSocketAddress) s.run();
    port = bindAddr.getPort();

    StreamContext issContext = new StreamContext(sink);
    issContext.setSerde(serde);


    StreamConfiguration sconf = new StreamConfiguration();
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", port));

    String upstreamNodeId = "upstreamId";
    String upstreamNodeType = "upstreamType";
    String downstreamNodeId = "downstreamId";
    String downstreamNodeType = "downstreamType";

    BufferServerInputSocketStream iss = new BufferServerInputSocketStream();
    iss.setup(sconf);
    iss.setContext(issContext, upstreamNodeId, upstreamNodeType, downstreamNodeId);
    System.out.println("input stream ready");

    BufferServerOutputSocketStream oss = new BufferServerOutputSocketStream();
    StreamContext ossContext = new StreamContext(null);
    ossContext.setSerde(serde);
    oss.setup(sconf);
    oss.setContext(ossContext, upstreamNodeId, upstreamNodeType);

    LOG.info("Sending hello message");
    oss.doSomething(StramTestSupport.generateBeginWindowTuple(upstreamNodeId, 0, ossContext));
    oss.doSomething(StramTestSupport.generateTuple("hello", 0, ossContext));
    oss.doSomething(StramTestSupport.generateEndWindowTuple(upstreamNodeId, 0, 1, ossContext));
    synchronized (SocketStreamTest.this) {
      if (messageCount.get() == 0) { // receiver could be done before we get here
        SocketStreamTest.this.wait(4000);
      }
    }

    Assert.assertEquals("Received messages", 1, messageCount.get());
    System.out.println("exiting...");

  }
}
