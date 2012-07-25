/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import kafka.message.Message;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@Ignore
public class InputKafkaStreamTest
{
  private static StreamConfiguration config;
  private static KafkaInputStream instance;
  private static MyStreamContext context;

  private static final class MyStreamContext extends StreamContext implements
    Sink
  {
    SerDe myserde;

    public MyStreamContext()
    {
      myserde = new DefaultSerDe();
    }

    @Override
    public SerDe getSerDe()
    {
      return myserde;
    }

    public void doSomething(Tuple t)
    {
      System.out.println("sinking " + t.getObject());
    }
  }

  public InputKafkaStreamTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    config = new StreamConfiguration();
    config.set("zk.connect", "localhost:2181");
    config.set("zk.connectiontimeout.ms", "1000000");
    config.set("groupid", "test_group");
    config.set("topic", "test");

    instance = new KafkaInputStream();
    context = new MyStreamContext();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    config = null;
    instance = null;
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
   * Test of setup method, of class KafkaInputStream.
   */
  public void testSetup()
  {
    System.out.println("setup");
    StreamConfiguration config = null;
    KafkaInputStream instance = new KafkaInputStream();
    instance.setup(config);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of setContext method, of class KafkaInputStream.
   */
  @Test
  public void testSetContext()
  {
    System.out.println("setContext");
    instance.setContext(context);
  }

  /**
   * Test of run method, of class KafkaInputStream.
   */
  public void testRun()
  {
    System.out.println("run");
    KafkaInputStream instance = new KafkaInputStream();
    instance.run();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getTuple method, of class KafkaInputStream.
   */
  public void testSendTuple()
  {
    System.out.println("getTuple");
    Message message = null;
    KafkaInputStream instance = new KafkaInputStream();
    instance.emit(message);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of teardown method, of class KafkaInputStream.
   */
  public void testTeardown()
  {
    System.out.println("teardown");
    KafkaInputStream instance = new KafkaInputStream();
    instance.teardown();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
}
