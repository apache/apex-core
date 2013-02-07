/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a non-premium implementation of circular buffer<p>
 * <br>
 *
 */
public class SynchronizedCircularBuffer<T> implements UnsafeBlockingQueue<T>
{
  private static final Logger logger = LoggerFactory.getLogger(SynchronizedCircularBuffer.class);
  private final T[] buffer;
  private final int buffermask;
  private int tail;
  private int head;
  private final int spinMillis;

  /**
   *
   * Constructing a circular buffer of 'n' integers<p>
   * <br>
   *
   * @param n size of the buffer to be constructed
   * @param spin time in milliseconds for which to wait before checking for expected value if it's missing
   * <br>
   */
  @SuppressWarnings("unchecked")
  public SynchronizedCircularBuffer(int n, int spin)
  {
    int i = 1;
    while (i < n) {
      i <<= 1;
    }

    buffer = (T[])new Object[i];
    buffermask = i - 1;

    spinMillis = spin;
  }

  /**
   *
   * Constructing a circular buffer of 'n' integers<p>
   * <br>
   *
   * @param n size of the buffer to be constructed
   * <br>
   */
  public SynchronizedCircularBuffer(int n)
  {
    this(n, 10);
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
  public synchronized boolean add(T toAdd)
  {
    if (head - tail <= buffermask) {
      buffer[head++ & buffermask] = toAdd;
      return true;
    }

    throw new IllegalStateException("Collection is full");
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
  public synchronized T remove()
  {
    if (head > tail) {
      return buffer[tail++ & buffermask];
    }

    throw new IllegalStateException("Collection is empty");
  }

  @Override
  public synchronized T peek()
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
  @Override
  public final synchronized int size()
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
  @Override
  public synchronized int drainTo(Collection<? super T> container)
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
  public synchronized String toString()
  {
    return "head=" + head + ", tail=" + tail + ", capacity=" + (buffermask + 1);
  }

  @Override
  public final synchronized boolean offer(T e)
  {
    if (head - tail <= buffermask) {
      buffer[head++ & buffermask] = e;
      return true;
    }

    return false;
  }

  @Override
  public final synchronized void put(T e) throws InterruptedException
  {
    do {
      if (head - tail < buffermask) {
        buffer[(int)(head++ & buffermask)] = e;
        return;
      }

      wait(spinMillis);
    }
    while (true);
  }

  @Override
  public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public T take() throws InterruptedException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int remainingCapacity()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean remove(Object o)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean contains(Object o)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public final synchronized T poll()
  {
    if (head > tail) {
      return buffer[tail++ & buffermask];
    }

    return null;
  }

  @Override
  public T element()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isEmpty()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<T> iterator()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object[] toArray()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <T> T[] toArray(T[] a)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean addAll(Collection<? extends T> c)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public final synchronized T pollUnsafe()
  {
    return buffer[tail++ & buffermask];
  }

  @Override
  public final synchronized T peekUnsafe()
  {
    return buffer[tail & buffermask];
  }
}
