/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CircularBufferTest
{
  private static final Logger logger = LoggerFactory.getLogger(CircularBufferTest.class);
  private static final long waitMillis = 500;

  public CircularBufferTest()
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

  @Before
  public void setUp()
  {
  }

  @After
  public void tearDown()
  {
  }

  /**
   * Test of add method, of class CircularBuffer.
   */
  @Test
  public void testAdd()
  {
    Thread.currentThread().setName("TestAdd");

    CircularBuffer<Integer> instance = new CircularBuffer<Integer>(0);
    Assert.assertEquals("capacity", instance.capacity(), 1);

    for (int i = 0; i < instance.capacity(); i++) {
      instance.add(i);
    }

    try {
      instance.add(new Integer(0));
      Assert.fail("exception should be raised for adding to buffer which does not have room");
    }
    catch (Exception bue) {
      assert (bue instanceof IllegalStateException);
    }

    instance = new CircularBuffer<Integer>(10);
    for (int i = 0; i < 10; i++) {
      instance.add(i);
    }
    assert (instance.size() == 10);

    for (int i = 10; i < instance.capacity(); i++) {
      instance.add(i);
    }

    try {
      instance.add(new Integer(0));
      Assert.fail("exception should have been thrown");
    }
    catch (Exception e) {
      assert (e instanceof IllegalStateException);
      instance.remove();
      instance.add(new Integer(0));
    }

    assert (instance.size() == instance.capacity());
  }

  /**
   * Test of remove method, of class CircularBuffer.
   */
  @Test
  public void testGet()
  {
    Thread.currentThread().setName("TestGet");

    CircularBuffer<Integer> instance = new CircularBuffer<Integer>(0);
    try {
      instance.remove();
      Assert.fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof IllegalStateException);
      assert (bue.getMessage().equals("Collection is empty"));
    }

    instance = new CircularBuffer<Integer>(10);
    try {
      instance.remove();
      Assert.fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof IllegalStateException);
      assert (bue.getMessage().equals("Collection is empty"));
    }

    for (int i = 0; i < 10; i++) {
      instance.add(i);
    }

    Integer i = instance.remove();
    Integer j = instance.remove();
    assert (i == 0 && j == 1);

    instance.add(10);

    assert (instance.size() == 9);
    assert (instance.remove() == 2);
  }

  @Test
  public void testPerformanceOfCircularBuffer() throws InterruptedException
  {
    testPerformanceOf(new CircularBuffer<Long>(1024 * 1024), 100);
    testPerformanceOf(new CircularBuffer<Long>(1024 * 1024), waitMillis);
  }

  @Test
  public void testPerformanceOfSynchronizedCircularBuffer() throws InterruptedException
  {
    testPerformanceOf(new SynchronizedCircularBuffer<Long>(1024 * 1024), 100);
    testPerformanceOf(new SynchronizedCircularBuffer<Long>(1024 * 1024), waitMillis);
  }

  private <T extends UnsafeBlockingQueue<Long>> void testPerformanceOf(final T buffer, long millis) throws InterruptedException
  {
    Thread producer = new Thread("Producer")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        long l = 0;
        try {
          do {
            int i = 0;
            while (i++ < 1024 && buffer.offer(l++)) {
            }
            if (i != 1025) {
              l--;
              Thread.sleep(10);
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
      }
    };

    Thread consumer = new Thread("Consumer")
    {
      @Override
      @SuppressWarnings("SleepWhileInLoop")
      public void run()
      {
        long l = 0;
        try {
          int size;
          do {
            if ((size = buffer.size()) == 0) {
              sleep(10);
            }
            else {
              while (size-- > 0) {
                Assert.assertEquals(l++, buffer.pollUnsafe().longValue());
              }
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
      }
    };

    producer.start();
    consumer.start();

    Thread.sleep(millis);

    producer.interrupt();
    consumer.interrupt();

    producer.join();
    consumer.join();

    logger.debug(buffer.getClass().getSimpleName() + "(" + buffer.toString() + ")");
  }
}
