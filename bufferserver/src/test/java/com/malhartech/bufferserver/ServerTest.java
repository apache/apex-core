/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import junit.framework.TestCase;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ServerTest extends TestCase
{
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
    System.out.println("run");
    SocketAddress result = instance.run();
    assertNotNull(result);
    assertTrue(((InetSocketAddress) result).getPort() != 0);
  }

}
