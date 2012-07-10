/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.dag;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.netty.ClientPipelineFactory;
import java.net.InetSocketAddress;
import java.util.Collections;
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

  public static class BufferServerInputSocketStream extends InputSocketStream
  {

    /**
     * Requires upstream node info to setup subscriber TODO: revisit context
     */
    public void setContext(StreamContext context, String upstreamNodeId, String upstreamNodeLogicalName, String downstreamNodeId)
    {
      super.setContext(context);
      String downstreamNodeLogicalName = "downstreamNodeLogicalName"; // TODO: why do we need this?
      ClientHandler.registerPartitions(channel, downstreamNodeId, downstreamNodeLogicalName, upstreamNodeId, upstreamNodeLogicalName, Collections.<String>emptyList());
    }
//    @Override
//    public ClientPipelineFactory getClientPipelineFactory()
//    {
//      return new ClientPipelineFactory(ClientHandler.class);
//    }
  }

  public static class BufferServerOutputSocketStream extends OutputSocketStream
  {

    public void setContext(com.malhartech.dag.StreamContext context, String upstreamNodeId, String upstreamNodeLogicalName)
    {
      super.setContext(context);

      // send publisher request
      LOG.info("registering publisher: {} {}", upstreamNodeId, upstreamNodeLogicalName);
      ClientHandler.publish(channel, upstreamNodeId, upstreamNodeLogicalName);
    }
//    @Override
//    public ClientPipelineFactory getClientPipelineFactory()
//    {
//      return new ClientPipelineFactory(ClientHandler.class);
//    }
  }

  /**
   * Send tuple on outputstream and receive tuple from inputstream
   *
   * @throws Exception
   */
  // @Ignore
  @Test
  public void test1() throws Exception
  {

    Sink sink = new Sink()
    {

      @Override
      public void doSomething(Tuple t)
      {
        System.out.println("received: " + t.getObject());
        synchronized (SocketStreamTest.this) {
          SocketStreamTest.this.notify();
        }
      }
    };

    SerDe serde = new InputActiveMQStreamTest.MySerDe();

    int port = 9080; //50001; // TODO: find random port
    //com.malhartech.bufferserver.Server s = new Server(port);
    //SocketAddress bindAddr  = s.run();
    //port = ((InetSocketAddress)bindAddr).getPort();


    StreamContext issContext = new StreamContext(sink);
    issContext.setSerde(serde);


    StreamConfiguration sconf = new StreamConfiguration();
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", port));

    BufferServerInputSocketStream iss = new BufferServerInputSocketStream();
    iss.setup(sconf);
    iss.setContext(issContext, "upstreamNodeId", "upstreamNodeLogicalId", "downStreamNodeId");
    System.out.println("input stream ready");

    BufferServerOutputSocketStream oss = new BufferServerOutputSocketStream();
    StreamContext ossContext = new StreamContext(null);
    ossContext.setSerde(serde);
    oss.setup(sconf);
    oss.setContext(ossContext, "upstreamNodeId", "upstreamNodeLogicalId");

    Tuple t = DataProcessingTest.generateTuple("hello", ossContext);
    LOG.info("Sending hello message");
    oss.doSomething(t);
    synchronized (this) {
      this.wait(2000);
    }
    System.out.println("exiting...");

  }
}
