/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.Sink;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public final class OutputSocketStreamTest
{
  static class MyOutputSocketStream extends SocketOutputStream
  {
    @Override
    public void flush(ChannelHandlerContext ctx, ChannelFuture future) throws Exception
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Sink connect(String port, Sink sink)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void process(Object payload)
    {
      throw new UnsupportedOperationException("Not supported yet.");
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

    ctx = new StreamContext("irrelevant_id");
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
    io.netty.channel.ChannelPipeline pipeline = oss.channel.pipeline();
    assert (pipeline.last() == oss);
  }
}
