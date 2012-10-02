/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.Buffer.Data.DataType;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
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
  public void testRun()
  {
    try {
      System.out.println("run");
      SocketAddress result = instance.run();
      assertNotNull(result);
      assertTrue(((InetSocketAddress)result).getPort() != 0);
    }
    catch (Exception ex) {
      LoggerFactory.getLogger(ServerTest.class).error(null, ex);
    }
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

  public void testPurge()
  {
    System.out.println("purge");
    try {
      SocketAddress result = instance.run();
      // create no tuples
      // ensure that no data is received
      // ensure that no data is received
    }
    catch (Exception ex) {
      Logger.getLogger(ServerTest.class.getName()).log(Level.SEVERE, null, ex);
    }

    // register publisher
    // register subscriber
    // ensure that no data is received

    // register publisher
    // register subscriber
    // publish a window
    // ensure that data is received

    // register subscriber
    // ensure that data is received

    // register publisher
    // register subscriber
    // publish a lot of data
    // ensure that all the data is received

    // purge most of it
    // register subscriber
    // ensure that the remanining data is received

    // purge all of it
    // register subscriber
    // ensure that no data is received

    // publish some more
    // register subscriber
    // ensure that the data is received

    for (int i = 0; i < 1000; i++) {
    }
  }
}
