/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import java.net.InetSocketAddress;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Ignore
public final class OutputSocketStreamTest
{
  static class MyOutputSocketStream extends SocketOutputStream 
  {
    @Override
    public void writeRequested(ChannelHandlerContext context, MessageEvent ev)
    {
      System.out.println(ev.getMessage().getClass().getName());
      
    }
  }
  
  static SocketOutputStream oss;
  static StreamConfiguration sc;
  static StreamContext ctx;
  public OutputSocketStreamTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    oss = new MyOutputSocketStream();
    sc = new StreamConfiguration();
    sc.setSocketAddr(StreamConfiguration.SERVER_ADDRESS, new InetSocketAddress("localhost", 5033));
    
    ctx = new StreamContext("irrelevant_source", "irrelevant_sink");
    ctx.setSerde(new DefaultSerDe());
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    oss = null;
    sc = null;
  }
  
  @Before
  public void setUp()
  {
    oss.setup(sc);
  }
  
  @After
  public void tearDown()
  {
    oss.teardown();
  }

  /**
   * Test of getClientPipelineFactory method, of class SocketOutputStream.
   */
  @Ignore
  @Test
  public void testGetClientPipelineFactory()
  {
    System.out.println("getClientPipelineFactory");
    ChannelPipeline pipeline = oss.channel.getPipeline();
    
    assert(pipeline.getLast() == oss);
    
  }
}
