/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import static org.junit.Assert.*;
import org.junit.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
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

  /**
   * Test of peek method, of class StablePriorityQueue.
   */
  @Test
  public void testPeek()
  {
    System.out.println("peek");
    StablePriorityQueue instance = null;
    Object expResult = null;
    Object result = instance.peek();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of remove method, of class StablePriorityQueue.
   */
  @Test
  public void testRemove_0args()
  {
    System.out.println("remove");
    StablePriorityQueue instance = null;
    Object expResult = null;
    Object result = instance.remove();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of poll method, of class StablePriorityQueue.
   */
  @Test
  public void testPoll()
  {
    System.out.println("poll");
    StablePriorityQueue instance = null;
    Object expResult = null;
    Object result = instance.poll();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of comparator method, of class StablePriorityQueue.
   */
  @Test
  public void testComparator()
  {
    System.out.println("comparator");
    StablePriorityQueue instance = null;
    Comparator expResult = null;
    Comparator result = instance.comparator();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of add method, of class StablePriorityQueue.
   */
  @Test
  public void testAdd()
  {
    System.out.println("add");
    Object e = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.add(e);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of size method, of class StablePriorityQueue.
   */
  @Test
  public void testSize()
  {
    System.out.println("size");
    StablePriorityQueue instance = null;
    int expResult = 0;
    int result = instance.size();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of isEmpty method, of class StablePriorityQueue.
   */
  @Test
  public void testIsEmpty()
  {
    System.out.println("isEmpty");
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.isEmpty();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of contains method, of class StablePriorityQueue.
   */
  @Test
  public void testContains()
  {
    System.out.println("contains");
    Object o = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.contains(o);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of iterator method, of class StablePriorityQueue.
   */
  @Test
  public void testIterator()
  {
    System.out.println("iterator");
    StablePriorityQueue instance = null;
    Iterator expResult = null;
    Iterator result = instance.iterator();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of toArray method, of class StablePriorityQueue.
   */
  @Test
  public void testToArray_0args()
  {
    System.out.println("toArray");
    StablePriorityQueue instance = null;
    Object[] expResult = null;
    Object[] result = instance.toArray();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of remove method, of class StablePriorityQueue.
   */
  @Test
  public void testRemove_Object()
  {
    System.out.println("remove");
    Object o = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.remove(o);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of containsAll method, of class StablePriorityQueue.
   */
  @Test
  public void testContainsAll()
  {
    System.out.println("containsAll");
    Collection<?> c = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.containsAll(c);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }


  /**
   * Test of removeAll method, of class StablePriorityQueue.
   */
  @Test
  public void testRemoveAll()
  {
    System.out.println("removeAll");
    Collection<?> c = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.removeAll(c);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of retainAll method, of class StablePriorityQueue.
   */
  @Test
  public void testRetainAll()
  {
    System.out.println("retainAll");
    Collection<?> c = null;
    StablePriorityQueue instance = null;
    boolean expResult = false;
    boolean result = instance.retainAll(c);
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of clear method, of class StablePriorityQueue.
   */
  @Test
  public void testClear()
  {
    System.out.println("clear");
    StablePriorityQueue instance = null;
    instance.clear();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }
}
