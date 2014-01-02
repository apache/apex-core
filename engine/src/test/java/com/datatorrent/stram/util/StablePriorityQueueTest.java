/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.util;

import static org.junit.Assert.*;
import org.junit.*;

import com.datatorrent.stram.util.StablePriorityQueue;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
@Ignore
public class StablePriorityQueueTest
{

  public StablePriorityQueueTest()
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
   * Test of element method, of class StablePriorityQueue.
   */
  @Test
  public void testElement()
  {
    System.out.println("element");
    StablePriorityQueue<Integer> instance = new StablePriorityQueue<Integer>(1);
    Integer i = 10;
    instance.add(i);
    Object result = instance.element();
    assertEquals(i, result);
  }

  /**
   * Test of offer method, of class StablePriorityQueue.
   */
  @Test
  public void testOffer()
  {
    System.out.println("offer");
    StablePriorityQueue<Integer> instance = new StablePriorityQueue<Integer>(1);
    Integer i = 10;
    assertTrue(instance.offer(i));
    Object result = instance.peek();
    assertEquals(i, result);
  }

}
