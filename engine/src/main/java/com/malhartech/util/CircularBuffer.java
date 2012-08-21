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
 * get() on the buffer consumes the object from tail<br>
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
  public CircularBuffer(int n)
  {
    buffer = (T[]) new Object[n];
    tail = 0;
    head = 0;
  }

  public void add(T toAdd)
  {
    if (head - tail == buffer.length) {
      throw overflow;
    }
    else {
      buffer[head++ % buffer.length] = toAdd;
    }
  }

  public T get()
  {
    if (head > tail) {
      return buffer[tail++ % buffer.length];
    }
    
    throw underflow;
  }

  public final int size()
  {
    return head - tail;
  }

  public int capacity()
  {
    return buffer.length;
  }

  public int drainTo(Collection<? super T> container)
  {
    int size = size();

    while (tail < head) {
      container.add(buffer[tail++]);
    }
    
    return size;
  }
  
  @Override
  public String toString()
  {
    return "CircularBuffer(capacity=" + buffer.length + ", head=" + head + ", tail=" + tail + ")";
  }
}