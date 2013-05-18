/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.common;

import java.util.ArrayList;
import java.util.PriorityQueue;
import javax.validation.constraints.Min;

/**
 *
 * Gives top N objects in ascending or descending order<p>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N. There is no API to look at top of the list at any given time as per design. The aim is for users to only take the topN
 * once all the inserts are done<br>
 *
 *
 * @author amol<br>
 *
 */
public class TopNSort<E>
{
  @Min(1)
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;
  PriorityQueue<E> q = null;

  /**
   * getter function for qbound
   * @return qbound
   */
  @Min(1)
  public int getQbound()
  {
    return qbound;
  }

  /**
   * setter function for qbound
   * @param i
   */
  public void setQbound(int i)
  {
    qbound = i;
  }

  /**
   * Added default constructor for deserializer
   */
  public TopNSort()
  {
  }

  /**
   * Constructs and sets values accordingly
   * @param initialCapacity
   * @param bound
   * @param flag
   */
  public TopNSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(flag));
    qbound = bound;
  }

  /**
   * adds an object
   * @param e
   * @return true is add succeeds
   */
  public boolean add(E e)
  {
    return offer(e);
  }

  /**
   * Size of the queue
   * @return size of the priority queue
   */
  public int size()
  {
    return q.size();
  }

  /**
   * Clears the queue
   */
  public void clear()
  {
    q.clear();
  }

  /**
   *
   * @return true if queue is empty
   */
  public boolean isEmpty()
  {
    return q.isEmpty();
  }

  /**
   * Returns topN objects
   * @param n
   * @return ArrayList of top N object
   */
  public ArrayList getTopN(int n)
  {
    ArrayList list = new ArrayList();
    E v;
    int j = 0;
    while ((v = q.poll()) != null) {
      list.add(v);
      j++;
      if (j > n) {
        break;
      }
    }
    if (list.isEmpty()) {
      return list;
    }

    ArrayList ret = new ArrayList(list.size());
    int size = list.size();
    int depth = size;
    if (depth > n) {
      depth = n;
    }
    for (int i = 0; i < depth; i++) {
      ret.add(list.get(size - i - 1));
    }
    return ret;
  }

  /**
   * Adds object
   * @param e object to be added
   * @return true is offer() succeeds
   */
  public boolean offer(E e)
  {
    if (q.size() <= qbound) {
      return q.offer(e);
    }

    boolean ret = true;
    boolean insert;
    Comparable<? super E> head = (Comparable<? super E>)q.peek();

    if (ascending) { // means head is the lowest value due to inversion
      insert = head.compareTo(e) <= 0; // e >= head
    }
    else { // means head is the highest value due to inversion
      insert = head.compareTo(e) >= 0; // head is <= e
    }
    if (insert && q.offer(e)) {
      ret = true;
      q.poll();
    }
    return ret;
  }
}
