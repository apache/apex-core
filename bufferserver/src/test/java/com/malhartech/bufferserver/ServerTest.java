/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import junit.framework.TestCase;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ServerTest extends TestCase
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ServerTest.class);

  public ServerTest(String testName)
  {
    super(testName);
  }

  @Override
  protected void setUp() throws Exception
  {
    super.setUp();

    instance = new Server(0);
  }

  @Override
  protected void tearDown() throws Exception
  {
    super.tearDown();

    instance.shutdown();
  }
  Server instance;

  /**
   * Test of run method, of class Server.
   */
  public void testRun() throws Exception
  {
    System.out.println("run");
    SocketAddress result = instance.run();
    assertNotNull(result);
    assertTrue(((InetSocketAddress)result).getPort() != 0);
  }

  private void testNoPublishNoSubscribe(BufferServerPublisher bsp, BufferServerSubscriber bss) throws InterruptedException
  {
    bsp.activate();
    bss.activate();

    Thread.sleep(100);

    bss.deactivate();
    bsp.deactivate();

    assert (bss.tupleCount.get() == 0);
  }

  private void test1Window(BufferServerPublisher bsp, BufferServerSubscriber bss) throws InterruptedException
  {
    bsp.activate();
    bss.activate();

    ResetTuple rt = new ResetTuple();
    rt.id = 0x7afebabe000000faL;
    bsp.publishMessage(rt);

    Thread.sleep(300);

    bss.deactivate();
    bsp.deactivate();

    assert (bss.tupleCount.get() == 1);
    assert (bss.firstPayload.getType() == rt.getType());
  }

  private void testLateSubscriber(BufferServerSubscriber bss) throws InterruptedException
  {
    bss.activate();

    Thread.sleep(100);

    bss.deactivate();

    assert (bss.tupleCount.get() == 1);
    assert (bss.firstPayload.getType() == DataType.RESET_WINDOW);
  }

  private void testATonOfData(BufferServerPublisher bsp, BufferServerSubscriber bss) throws InterruptedException
  {
    bsp.activate();
    bss.activate();

    BeginTuple bt = new BeginTuple();
    bt.id = 0x7afebabe00000000L;
    bsp.publishMessage(bt);

    for (int i = 0; i < 100; i++) {
      bsp.publishMessage(new byte[i]);
    }

    EndTuple et = new EndTuple();
    et.id = bt.id;
    bsp.publishMessage(et);

    BeginTuple bt1 = new BeginTuple();
    bt1.id = bt.id + 1;
    bsp.publishMessage(bt1);

    for (int i = 0; i < 100; i++) {
      bsp.publishMessage(new byte[i]);
    }

    EndTuple et1 = new EndTuple();
    et1.id = bt1.id;
    bsp.publishMessage(et1);

    Thread.sleep(100);

    bsp.deactivate();
    bss.deactivate();

    assert (bss.tupleCount.get() == 205);
  }

  private void testPurgeSome(BufferServerController bsc, BufferServerSubscriber bss) throws InterruptedException
  {
    bsc.windowId = 0;
    bsc.activate();
    Thread.sleep(1000);
    bsc.deactivate();

//    assert(bsc.data != null);
  }

  class ResetTuple implements Tuple
  {
    long id;

    public DataType getType()
    {
      return DataType.RESET_WINDOW;
    }

    public long getWindowId()
    {
      return id;
    }

    public int getIntervalMillis()
    {
      return (int)id;
    }

    public int getBaseSeconds()
    {
      return (int)(id >> 32);
    }
  }

  class BeginTuple extends ResetTuple
  {
    @Override
    public DataType getType()
    {
      return DataType.BEGIN_WINDOW;
    }
  }

  class EndTuple extends ResetTuple
  {
    @Override
    public DataType getType()
    {
      return DataType.END_WINDOW;
    }
  }

  public void testPurge() throws Exception
  {
    System.out.println("purge");
    SocketAddress result = instance.run();
    assert (result instanceof InetSocketAddress);
    String host = ((InetSocketAddress)result).getHostName();
    int port = ((InetSocketAddress)result).getPort();

    BufferServerPublisher bsp = new BufferServerPublisher("MyPublisher");
    bsp.setup(host, port);

    BufferServerSubscriber bss = new BufferServerSubscriber("MyPublisher", null);
    bss.setup(host, port);

    BufferServerController bsc = new BufferServerController("MyPublisher");
    bsc.setup(host, port);

    testNoPublishNoSubscribe(bsp, bss);

    test1Window(bsp, bss);

    testLateSubscriber(bss);

    testATonOfData(bsp, bss);

    testPurgeSome(bsc, bss);

    // purge all of it
    // register subscriber
    // ensure that no data is received

    // publish some more
    // register subscriber
    // ensure that the data is received
    logger.debug("done!");
  }
}
