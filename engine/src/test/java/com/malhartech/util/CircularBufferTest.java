/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import static org.junit.Assert.fail;
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
    try {
      instance.add(new Integer(0));
      fail("exception should be raised for adding to buffer which does not have room");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferOverflowException);
    }

    instance = new CircularBuffer<Integer>(10);
    for (int i = 0; i < 10; i++) {
      instance.add(i);
    }

    assert (instance.size() == 10);
    try {
      instance.add(new Integer(0));
      fail("exception should have been thrown");
    }
    catch (Exception e) {
      assert (e instanceof BufferOverflowException);
      instance.get();
      instance.add(new Integer(0));
    }

    assert (instance.size() == 10);
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
      fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferUnderflowException);
    }

    instance = new CircularBuffer<Integer>(10);
    try {
      instance.get();
      fail("exception should be raised for getting from buffer which does not have data");
    }
    catch (Exception bue) {
      assert (bue instanceof BufferUnderflowException);
    }

    for (int i = 0; i < 10; i++) {
      instance.add(i);
    }

    Integer i = instance.get();
    Integer j = instance.get();
    assert(i == 0 && j == 1);

    instance.add(10);

    assert(instance.size() == 9);
    assert(instance.get() == 2);
  }
}
