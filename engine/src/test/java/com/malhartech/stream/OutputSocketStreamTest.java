/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
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
    public Sink setSink(String port, Sink sink)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void process(Object payload)
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isMultiSinkCapable()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
  static SocketOutputStream oss;
  static StreamContext ctx;

  public OutputSocketStreamTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    oss = new MyOutputSocketStream();
    ctx = new StreamContext("irrelevant_id");
    ctx.setBufferServerAddress(new InetSocketAddress("localhost", 5033));
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    oss = null;
  }

  @Before
  public void setUp()
  {
    oss.setup(ctx);
  }

  @After
  public void tearDown()
  {
    oss.teardown();
  }

  /**
   * Test of getClientPipelineFactory method, of class SocketOutputStream.
   */
  @Test
  @Ignore
  public void testGetClientPipelineFactory()
  {
    System.out.println("getClientPipelineFactory");
    oss.activate(ctx);
    io.netty.channel.ChannelPipeline pipeline = oss.channel.pipeline();
    assert (pipeline.last() == oss);
    oss.deactivate();
  }
}
