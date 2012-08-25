/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.*;


/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Ignore
public class InputActiveMQStreamTest
{

  static StreamConfiguration config;
  static AbstractActiveMQInputStream instance;
  static StreamContext context;


  private static final class MyStreamContext extends StreamContext implements Sink
  {

    DefaultSerDe myserde;

    public MyStreamContext()
    {
      super("irrelevant_source", "irrelevant_sink");
      myserde = new DefaultSerDe();
    }

    @Override
    public SerDe getSerDe()
    {
      return myserde;
    }

    public Sink getSink()
    {
      return this;
    }

    /**
     *
     * @param t the value of t
     */
    @Override
    public void sink(Object t)
    {
      System.out.println("sinking " + t.getObject());
    }
  }

  private static final class InputActiveMQStream extends AbstractActiveMQInputStream
  {

    @Override
    public Object getObject(Object object)
    {
      if (object instanceof TextMessage) {
        try {
          return ((TextMessage) object).getText();
        }
        catch (JMSException ex) {
          Logger.getLogger(InputActiveMQStreamTest.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
      return null;
    }
  }

  public InputActiveMQStreamTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    config = new StreamConfiguration();
    config.set("user", "");
    config.set("password", "");
    config.set("url", "tcp://localhost:61616");
    config.set("ackMode", "AUTO_ACKNOWLEDGE");
    config.set("clientId", "consumer1");
    config.set("consumerName", "ChetanConsumer");
    config.set("durable", "false");
    config.set("maximumMessages", "10");
    config.set("pauseBeforeShutdown", "true");
    config.set("receiveTimeOut", "0");
    config.set("sleepTime", "1000");
    config.set("subject", "TEST.FOO");
    config.set("parallelThreads", "1");
    config.set("topic", "false");
    config.set("transacted", "false");
    config.set("verbose", "true");
    config.set("batch", "10");

    instance = new InputActiveMQStream();

    context = new MyStreamContext();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    instance = null;
    config = null;
    context = null;
  }

  @Before
  public void setUp()
  {
    instance.setup(config);
  }

  @After
  public void tearDown()
  {
    instance.teardown();
  }

  /**
   * Test of setup method, of class AbstractActiveMQInputStream.
   */
  @Test
  public void testSetup()
  {
    System.out.println("setup");

    assertNotNull(instance.getConnection());
    assertNotNull(instance.getConsumer());
    assertNotNull(instance.getSession());
  }

  /**
   * Test of teardown method, of class AbstractActiveMQInputStream.
   */
  @Test
  public void testTeardown()
  {
    System.out.println("teardown");

    instance.teardown();
    assertNull(instance.getConnection());
    assertNull(instance.getConsumer());
    assertNull(instance.getSession());

    instance.setup(config); // make sure that test framework's teardown method
    // does not fail.
  }

  @Test
  public void testProcess()
  {
    System.out.println("process");
//    instance.setContext(context);
    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException ex) {
      Logger.getLogger(InputActiveMQStreamTest.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
