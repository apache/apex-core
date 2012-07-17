/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import static org.junit.Assert.fail;

import java.net.InetSocketAddress;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.MessageEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.SimpleData;
import com.malhartech.dag.DefaultSerDe;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Ignore
public final class OutputSocketStreamTest
{
  static class MyOutputSocketStream extends OutputSocketStream 
  {
    @Override
    public void writeRequested(ChannelHandlerContext context, MessageEvent ev)
    {
      System.out.println(ev.getMessage().getClass().getName());
      
    }
  }
  
  static OutputSocketStream oss;
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
    
    ctx = new StreamContext();
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
   * Test of doSomething method, of class OutputSocketStream.
   */
  @Test
  public void testDoSomething() throws InterruptedException
  {
    System.out.println("doSomething");
    oss.setContext(ctx);

    Data.Builder data = Data.newBuilder();
    data.setType(Data.DataType.SIMPLE_DATA).setWindowId(0);
    data.setSimpledata(SimpleData.newBuilder().setData(ByteString.EMPTY).build());
    
    Tuple t = new Tuple("hello");
    t.setData(data.build());
    t.setContext(oss.getContext());
    
    oss.doSomething(t);
    // TODO review the generated test code and remove the default call to fail.
    
    synchronized (oss) {
      oss.wait();
    }
    fail("The test case is a prototype.");
  }

  /**
   * Test of getClientPipelineFactory method, of class OutputSocketStream.
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
