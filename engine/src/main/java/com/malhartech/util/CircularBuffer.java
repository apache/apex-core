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
  //static {
  //  System.err.println("CircularBuffer clinit " + Thread.currentThread());
  //  Thread.dumpStack();
  //}

  private static final BufferUnderflowException underflow = new BufferUnderflowException();
  private static final BufferOverflowException overflow = new BufferOverflowException();
  private final T[] buffer;
  private final int buffermask;
  private volatile int tail;
  private volatile int head;

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
    int i = 1;
    while (i < n) {
      i = i << 1;
    }

    buffer = (T[])new Object[i];
    buffermask = i - 1;
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
    if (head - tail <= buffermask) {
      buffer[head++ & buffermask] = toAdd;
      return;
    }

    throw overflow;
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
    if (head > tail) {
      return buffer[tail++ & buffermask];
    }

    throw underflow;
  }

  public T peek()
  {
    if (head > tail) {
      return buffer[tail & buffermask];
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
    return head - tail;
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
    return buffermask + 1;
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

    while (head > tail) {
      container.add(buffer[tail++ & buffermask]);
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
    return "CircularBuffer(capacity=" + (buffermask + 1) + ", head=" + head + ", tail=" + tail + ")";
  }
}
