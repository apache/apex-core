/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.malhartech.bufferserver.Server;


/**
 *
 */
public class SocketStreamTest {

  /**
   * Send tuple on outputstream and receive tuple from inputstream
   * @throws Exception
   */
 // @Ignore
  @Test
  public void test1() throws Exception {

    Sink sink = new Sink() {
      @Override
      public void doSomething(Tuple t) {
        System.out.println("received: " + t.getObject());
      }
    };
    
    SerDe serde = new InputActiveMQStreamTest.MySerDe();
    
    int port = 50001; // TODO: find random port
    com.malhartech.bufferserver.Server s = new Server(port);
    s.run();

    StreamContext issContext = new StreamContext(sink);
    issContext.setSerde(serde);
    StreamConfiguration sconf = new StreamConfiguration();
    sconf.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, InetSocketAddress.createUnresolved("localhost", port));
    
    InputSocketStream iss = new InputSocketStream();
    iss.setContext(issContext);
    iss.setup(sconf);
    System.out.println("input stream ready");

    OutputSocketStream oss = new OutputSocketStream();
    StreamContext ossContext = new StreamContext(null);
    ossContext.setSerde(serde);
    oss.setContext(ossContext); 
    oss.setup(sconf);

    Tuple t = DataProcessingTest.generateTuple("hello", ossContext);
    oss.doSomething(t);
    //Thread.sleep(1000000);
    System.out.println("exiting...");
    
  }
  
}
