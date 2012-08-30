/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.Buffer;
import com.malhartech.dag.*;
import com.malhartech.stram.ManualScheduledExecutorService;
import com.malhartech.stram.WindowGenerator;
import org.apache.hadoop.conf.Configuration;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class AbstractInputAdapterTest
{
  public AbstractInputAdapterTest()
  {
  }

  @BeforeClass
  public static void setUpClass() throws Exception
  {
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
  }
  @SuppressWarnings("PackageVisibleField")
  AbstractInputNode instance;
  StreamContext context;

  @Before
  public void setUp()
  {
    context = new StreamContext("irrelevant_id");
    instance = new AbstractInputAdapterImpl();
  }

  @After
  public void tearDown()
  {
    instance = null;
  }

  /**
   * Test of resetWindow method, of class AbstractInputAdapter.
   */
  @Test
  public void testResetWindow()
  {
    System.out.println("resetWindow");

    ManualScheduledExecutorService msse = new ManualScheduledExecutorService(1);
    msse.setCurrentTimeMillis(0xcafebabe * 1000L);
    WindowGenerator generator = new WindowGenerator(msse);

    final Configuration config = new Configuration();
    config.setLong(WindowGenerator.FIRST_WINDOW_MILLIS, msse.getCurrentTimeMillis());
    config.setInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 0x1234abcd);

    generator.setup(config);
    generator.connect("output", new Sink()
    {
      boolean firsttime = true;

      @Override
      public void process(Object payload)
      {
        if (firsttime) {
          assert (payload instanceof ResetWindowTuple);
          assert (((ResetWindowTuple)payload).getWindowId() == 0xcafebabe00000000L);
          assert (((ResetWindowTuple)payload).getBaseSeconds() * 1000L == config.getLong(WindowGenerator.FIRST_WINDOW_MILLIS, 0));
          assert (((ResetWindowTuple)payload).getIntervalMillis() == config.getInt(WindowGenerator.WINDOW_WIDTH_MILLIS, 0));
          firsttime = false;
        }
        else {
          assert (payload instanceof Tuple);
          assert (((Tuple)payload).getWindowId() == 0xcafebabe00000000L);
        }
      }
    });

    generator.activate(null);
    msse.tick(1);
  }

//  /**
//   * Test of beginWindow method, of class AbstractInputAdapter.
//   */
//  @Test
//  public void testBeginWindow()
//  {
//    System.out.println("beginWindow");
//    int windowId = 0;
//    AbstractInputAdapter instance = new AbstractInputAdapterImpl();
//    instance.beginWindow(windowId);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of endWindow method, of class AbstractInputAdapter.
//   */
//  @Test
//  public void testEndWindow()
//  {
//    System.out.println("endWindow");
//    int windowId = 0;
//    AbstractInputAdapter instance = new AbstractInputAdapterImpl();
//    instance.endWindow(windowId);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of endStream method, of class AbstractInputAdapter.
//   */
//  @Test
//  public void testEndStream()
//  {
//    System.out.println("endStream");
//    AbstractInputAdapter instance = new AbstractInputAdapterImpl();
//    instance.endStream();
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
//
//  /**
//   * Test of hasFinished method, of class AbstractInputAdapter.
//   */
//  @Test
//  public void testHasFinished()
//  {
//    System.out.println("hasFinished");
//    AbstractInputAdapter instance = new AbstractInputAdapterImpl();
//    boolean expResult = false;
//    boolean result = instance.hasFinished();
//    assertEquals(expResult, result);
//    // TODO review the generated test code and remove the default call to fail.
//    fail("The test case is a prototype.");
//  }
  @SuppressWarnings("PublicInnerClass")
  public class AbstractInputAdapterImpl extends AbstractInputNode
  {
    @Override
    public void beginWindow()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void endWindow()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
