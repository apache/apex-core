/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class CircularBufferTest
{
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
    System.out.println("add");

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
      instance.get();
      instance.add(new Integer(0));
    }

    assert (instance.size() == instance.capacity());
  }

  /**
   * Test of get method, of class CircularBuffer.
   */
  @Test
  public void testGet()
  {
    System.out.println("get");

    CircularBuffer<Integer> instance = new CircularBuffer<Integer>(0);
    try {
      instance.get();
      Assert.fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferUnderflowException);
    }

    instance = new CircularBuffer<Integer>(10);
    try {
      instance.get();
      Assert.fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferUnderflowException);
    }

    for (int i = 0; i < 10; i++) {
      instance.add(i);
    }

    Integer i = instance.get();
    Integer j = instance.get();
    assert (i == 0 && j == 1);

    instance.add(10);

    assert (instance.size() == 9);
    assert (instance.get() == 2);
  }

  @Test
  public void testVolatile() throws InterruptedException
  {
    Thread.currentThread().setName("TestVolatile");
    final CircularBuffer<Long> buffer = new CircularBuffer<Long>(1024 * 1024);
    final long spinMillis = 35;
    Thread producer = new Thread("Producer")
    {
      @Override
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
              sleep(spinMillis);
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
        System.out.println("Produced " + l + " Longs");
      }
    };

    Thread consumer = new Thread("Consumer")
    {
      @Override
      public void run()
      {
        long l = 0;
        try {
          int size;
          do {
            if ((size = buffer.size()) == 0) {
              sleep(spinMillis);
            }
            else {
              while (size-- > 0) {
                Long ll;
//                buffer.get();
//                l++;
                while (true) {
                  ll = buffer.peek();
                  if (ll.longValue() == l) {
                    break;
                  }
                  else {
                    sleep(spinMillis);
                  }
                }

                buffer.get();
                Assert.assertEquals(l++, ll.longValue());
              }
            }
          }
          while (!interrupted());
        }
        catch (InterruptedException ex1) {
        }
        System.out.println("Consumed " + l + " Longs");
      }
    };

    producer.start();
    consumer.start();

    Thread.sleep(30000);

    producer.interrupt();
    consumer.interrupt();

    producer.join();
    consumer.join();
  }
}
