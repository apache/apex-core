/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.Server;
import com.malhartech.dag.StramTestSupport.MySerDe;

/**
 *
 */
public class SocketStreamTest
{

  private static Logger LOG = LoggerFactory.getLogger(SocketStreamTest.class);
  private static int bufferServerPort = 0;
  private static Server bufferServer = null;
  
  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    //   java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.FINEST);
    //    java.util.logging.Logger.getLogger("").info("test");
    bufferServer = new Server(0); // find random port
    InetSocketAddress bindAddr  = (InetSocketAddress)bufferServer.run();
    bufferServerPort = bindAddr.getPort();
  
  }

  @AfterClass
  public static void tearDown() throws IOException {
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
        System.out.println("received: " + t.getObject());
        synchronized (SocketStreamTest.this) {
          messageCount.incrementAndGet();
          SocketStreamTest.this.notifyAll();
        }
      }
    };

    SerDe serde = new MySerDe();


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

    Tuple t = StramTestSupport.generateTuple("hello", ossContext);
    LOG.info("Sending hello message");
    oss.doSomething(t);
    synchronized (SocketStreamTest.this) {
      if (messageCount.get() == 0) { // receiver could be done before we get here
        SocketStreamTest.this.wait(3000);
      }
    }
    
    Assert.assertEquals("Received messages", 1, messageCount.get());
    System.out.println("exiting...");

  }
}
