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
  private static InputKafkaStream instance;
  private static MyStreamContext context;

  private static final class MySerDe implements SerDe
  {

    public Object fromByteArray(byte[] bytes)
    {
      return new String(bytes);
    }

    public byte[] toByteArray(Object o)
    {
      return ((String) o).getBytes();
    }

    public byte[] getPartition(Object o)
    {
      return null;
    }
  }

  private static final class MyStreamContext extends StreamContext implements Sink
  {

    MySerDe myserde;

    public MyStreamContext()
    {
      myserde = new MySerDe();
    }

    @Override
    public SerDe getSerDe()
    {
      return myserde;
    }

    @Override
    public Sink getSink()
    {
      return this;
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

    instance = new InputKafkaStream();
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
   * Test of setup method, of class InputKafkaStream.
   */
  public void testSetup()
  {
    System.out.println("setup");
    StreamConfiguration config = null;
    InputKafkaStream instance = new InputKafkaStream();
    instance.setup(config);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of setContext method, of class InputKafkaStream.
   */
  @Test
  public void testSetContext()
  {
    System.out.println("setContext");
    instance.setContext(context);
  }

  /**
   * Test of run method, of class InputKafkaStream.
   */
  public void testRun()
  {
    System.out.println("run");
    InputKafkaStream instance = new InputKafkaStream();
    instance.run();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of getTuple method, of class InputKafkaStream.
   */
  public void testGetTuple()
  {
    System.out.println("getTuple");
    Message message = null;
    InputKafkaStream instance = new InputKafkaStream();
    Tuple expResult = null;
    Tuple result = instance.getTuple(message);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of teardown method, of class InputKafkaStream.
   */
  public void testTeardown()
  {
    System.out.println("teardown");
    InputKafkaStream instance = new InputKafkaStream();
    instance.teardown();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
}
