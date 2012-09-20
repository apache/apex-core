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
 *
 */
public class CircularBuffer<T>
{
  private static final BufferUnderflowException underflow = new BufferUnderflowException();
  private static final BufferOverflowException overflow = new BufferOverflowException();
  private final T[] buffer;
  private final int bufferlen;
  private volatile int tail;
  private volatile int head;
  private volatile int count;

  /**
   *
   * Constructing a circular buffer of 'n' integers<p>
   * <br>
   *
   * @param n size of the buffer to be constructed
   * <br>
   */
  @SuppressWarnings("unchecked")
  public CircularBuffer(int n)
  {
    buffer = (T[])new Object[n];
    bufferlen = n;
  }

  /**
   *
   * Add object at the head<p>
   * <br>
   *
   * @param toAdd object to be added
   *
   */
  public void add(T toAdd)
  {
    if (count == bufferlen) {
      throw overflow;
    }

    buffer[head++ % bufferlen] = toAdd;
    count++;
  }

  /**
   *
   * Get object from the tail<p>
   * <br>
   *
   * @return object removed from the buffer returned
   * <br>
   */
  public T get()
  {
    if (count > 0) {
      count--;
      return buffer[tail++ % bufferlen];
    }

    throw underflow;
  }

  public T peek()
  {
    if (count > 0) {
      return buffer[tail % bufferlen];
    }

    return null;
  }

  /**
   *
   * Number of objects in the buffer<p>
   * <br>
   *
   * @return Number of objects in the buffer
   * <br>
   */
  public final int size()
  {
    return count;
  }

  /**
   *
   * Total design capacity of the buffer<p>
   * <br>
   *
   * @return Total return capacity of the buffer
   * <br>
   */
  public int capacity()
  {
    return bufferlen;
  }

  /**
   *
   * Drain the buffer<p>
   * <br>
   *
   * @param container {@link java.util.Collection} class to which the buffer objects are added
   * @return Number of objects removed from the buffer
   * <br>
   */
  public int drainTo(Collection<? super T> container)
  {
    int size = size();

    while (count-- > 0) {
      container.add(buffer[tail++ % bufferlen]);
    }

    return size;
  }

  /**
   *
   * Printing status for debugging<p>
   * <br>
   *
   * @return String containing capacity, head, and tail
   * <br>
   */
  @Override
  public String toString()
  {
    return "CircularBuffer(capacity=" + bufferlen + ", count=" + count + ", head=" + head + ", tail=" + tail + ")";
  }
}
