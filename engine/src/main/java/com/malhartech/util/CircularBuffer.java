/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.Collection;

/**
 * Provides a circular buffer<p>
 * <br>
 * get() gets objects from the tail and increments the tail<br>
 * add() adds to the head<br>
 *
 */

public class CircularBuffer<T>
{
  private static final BufferUnderflowException underflow = new BufferUnderflowException();
  private static final BufferOverflowException overflow = new BufferOverflowException();
  private T[] buffer;
  private int tail;
  private int head;

  @SuppressWarnings("unchecked")
    
  /**
   * 
   * Constructing a circular buffer of 'n' integers<p>
   * <br>
   * @param n: size of the buffer to be constructed
   * <br>
   */
  public CircularBuffer(int n)
  {
    buffer = (T[]) new Object[n];
    tail = 0;
    head = 0;
  }

  /**
   * 
   * Add object at the head<p>
   * <br>
   * @param T: object to be added
   * 
   */
  public void add(T toAdd)
  {
    if (head - tail == buffer.length) {
      throw overflow;
    }
    else {
      buffer[head++ % buffer.length] = toAdd;
    }
  }

  /**
   * 
   * Get object from the tail<p>
   * <br>
   * @return T: object removed from the buffer returned
   * <br>
   */
  public T get()
  {
    if (head > tail) {
      return buffer[tail++ % buffer.length];
    }

    throw underflow;
  }

  /**
   * 
   * Number of objects in the buffer<p>
   * <br>
   * @return int: Number of objects in the buffer
   * <br>
   */
  public final int size()
  {
    return head - tail;
  }

  /**
   * 
   * Total design capacity of the buffer<p>
   * <br>
   * @return int: Total return capacity of the buffer
   * <br>
   */
  public int capacity()
  {
    return buffer.length;
  }

  /**
   * 
   * Drain the buffer<p>
   * <br>
   * @param T: {@link java.util.Collection} class to which the buffer objects are added
   * @return int: Number of objects removed from the buffer
   * <br>
   */
  public int drainTo(Collection<? super T> container)
  {
    int size = size();

    while (tail < head) {
      container.add(buffer[tail++ % buffer.length]);
    }

    return size;
  }

  /**
   * 
   * Printing status for debugging<p>
   * <br>
   * @return String: String contains capacity, head, and tail
   * <br>
   */
  @Override
  public String toString()
  {
    return "CircularBuffer(capacity=" + buffer.length + ", head=" + head + ", tail=" + tail + ")";
  }
}