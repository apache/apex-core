/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.concurrent.BlockingQueue;
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
      assert (bue instanceof BufferOverflowException);
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
      assert (e instanceof BufferOverflowException);
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
      assert (bue instanceof BufferUnderflowException);
    }

    instance = new CircularBuffer<Integer>(10);
    try {
      instance.remove();
      Assert.fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferUnderflowException);
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
    Thread.currentThread().setName("testPerformanceOfCircularBuffer");
    testPerformanceOf(new CircularBuffer<Long>(1024 * 1024), 500);
    testPerformanceOf(new CircularBuffer<Long>(1024 * 1024), 500);
  }

  @Test
  public void testPerformanceOfSynchronizedCircularBuffer() throws InterruptedException
  {
    Thread.currentThread().setName("testPerformanceOfSynchronizedCircularBuffer");
    testPerformanceOf(new SynchronizedCircularBuffer<Long>(1024 * 1024), 500);
    testPerformanceOf(new SynchronizedCircularBuffer<Long>(1024 * 1024), 500);
  }

  private <T extends BlockingQueue<Long>> void testPerformanceOf(final T buffer, long millis) throws InterruptedException
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
            try {
              for (int i = 0; i < 1024; i++) {
                buffer.add(l++);
              }
            }
            catch (BufferOverflowException ex) {
              l--;
              sleep(10);
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
        logger.debug("Produced {} Longs", l);
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
                Assert.assertEquals(l++, buffer.remove().longValue());
              }
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
        logger.debug("Consumed {} Longs", l);
      }
    };

    producer.start();
    consumer.start();

    Thread.sleep(millis);

    producer.interrupt();
    consumer.interrupt();

    producer.join();
    consumer.join();
  }
}
