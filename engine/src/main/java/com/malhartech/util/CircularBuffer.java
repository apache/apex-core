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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a premium implementation of circular buffer<p>
 * <br>
 *
 */
public class CircularBuffer<T> implements CBuffer<T>
{
  private static final Logger logger = LoggerFactory.getLogger(CircularBuffer.class);
  private final T[] buffer;
  private final int buffermask;
  private volatile long tail;
  private volatile long head;

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
  @Override
  public void add(T toAdd)
  {
    if (head - tail <= buffermask) {
      buffer[(int)(head & buffermask)] = toAdd;
      head++;
      return;
    }

    throw new BufferOverflowException();
  }

  /**
   *
   * Get object from the tail<p>
   * <br>
   *
   * @return object removed from the buffer returned
   * <br>
   */
  @Override
  public T get()
  {
    if (head > tail) {
      T t = buffer[(int)(tail & buffermask)];
      tail++;
      return t;
    }

    throw new BufferUnderflowException();
  }

  public T peek()
  {
    if (head > tail) {
      return buffer[(int)(tail & buffermask)];
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
  @Override
  public final int size()
  {
    return (int)(head - tail);
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
      container.add(buffer[(int)(tail & buffermask)]);
      tail++;
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
