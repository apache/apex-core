/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.util;

import java.lang.reflect.Array;
import java.util.*;

/**
 * 
 * Implements a priority queue by implementing {@link java.util.queue} interface<p>
 * <br>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class StablePriorityQueue<E> implements Queue<E>
{

  private final PriorityQueue<StableWrapper<E>> queue;
  private static final long serialVersionUID = 201207091837L;
  private int counter = 0;

  /**
   * 
   * Constructs a {@link com.malhartech.util.StablePriorityQueue} class<p>
   * <br>
   * @param initialCapacity The size of the queue to be set up
   * <br>
   */
  public StablePriorityQueue(int initialCapacity)
  {
    queue = new PriorityQueue<StableWrapper<E>>(initialCapacity, new StableWrapperNaturalComparator<E>());
  }

  /**
   * 
   * Constructs a {@link com.malhartech.util.StablePriorityQueue} class by absorbing all objects from a {@link java.util.Collection} object<p>
   * <br>
   * @param c a {@link java.util.Collection} object
   * <br>
   */
  public StablePriorityQueue(Collection<? extends E> c)
  {
    queue = new PriorityQueue<StableWrapper<E>>(c.size(), new StableWrapperNaturalComparator<E>());
    for (E e : c) {
      queue.add(new StableWrapper<E>(e, counter++));
    }
  }

  /**
   * 
   * Constructs a {@link com.malhartech.util.StablePriorityQueue} class with provided capacity<p>
   * <br>
   * @param initialCapacity Size of the queue to be set up
   * @param comparator {@link java.util.Comparator} object for comparison
   * <br>
   */
  public StablePriorityQueue(int initialCapacity, Comparator<? super E> comparator)
  {
    queue = new PriorityQueue<StableWrapper<E>>(initialCapacity, new StableWrapperProvidedComparator<E>(comparator));
  }

  @SuppressWarnings("unchecked")
  public StablePriorityQueue(StablePriorityQueue<? extends E> c)
  {
    queue = new PriorityQueue<StableWrapper<E>>(c.size(), (Comparator<? super StableWrapper<E>>) c.comparator());
  }

  public StablePriorityQueue(SortedSet<? extends E> c)
  {
    this((Collection<? extends E>) c);
  }

  @Override
  public E element() throws NoSuchElementException
  {
    try {
      return queue.element().object;
    }
    catch (NoSuchElementException nsee) {
      counter = 0;
      throw nsee;
    }
  }

  @Override
  public boolean offer(E e)
  {
    return queue.offer(new StableWrapper<E>(e, counter++));
  }

  @Override
  public E peek()
  {
    StableWrapper<E> sw = queue.peek();
    if (sw == null) {
      return null;
    }
    
    return sw.object;
  }

  @Override
  public E remove() throws NoSuchElementException
  {
    try {
      return queue.remove().object;
    }
    catch (NoSuchElementException nsee) {
      counter = 0;
      throw nsee;
    }
  }

  @Override
  public E poll()
  {
    StableWrapper<E> sw = queue.poll();
    if (sw == null) {
      return null;
    }
    
    return sw.object;
  }

  @SuppressWarnings("unchecked")
  public Comparator<? super E> comparator()
  {
    Comparator<? super StableWrapper<E>> comparator = queue.comparator();
    if (comparator instanceof StableWrapperProvidedComparator) {
      return ((StableWrapperProvidedComparator) comparator).comparator;
    }

    return null;
  }

  @Override
  public boolean add(E e)
  {
    return queue.add(new StableWrapper<E>(e, counter++));
  }

  @Override
  public int size()
  {
    int size = queue.size();
    if (size == 0) {
      counter = 0;
    }

    return size;
  }

  @Override
  public boolean isEmpty()
  {
    boolean isEmpty = queue.isEmpty();
    if (isEmpty) {
      counter = 0;
    }

    return isEmpty;
  }

  @Override
  public boolean contains(Object o)
  {
    for (StableWrapper<E> e : queue) {
      if (e.object == o) {
        return true;
      }
    }

    return false;
  }

  private final class IteratorWrapper implements Iterator<E>
  {

    final Iterator<StableWrapper<E>> iterator;

    public IteratorWrapper()
    {
      iterator = queue.iterator();
    }

    @Override
    public boolean hasNext()
    {
      return iterator.hasNext();
    }

    @Override
    public E next()
    {
      return iterator.next().object;
    }

    @Override
    public void remove()
    {
      iterator.remove();
    }
  }

  @Override
  public Iterator<E> iterator()
  {
    return new IteratorWrapper();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object[] toArray()
  {
    Object[] array = queue.toArray();

    for (int i = array.length; i-- > 0;) {
      array[i] = ((StableWrapper<E>) array[i]).object;
    }

    return array;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] a)
  {
    T[] finalArray;

    queue.toArray(a);

    final int length = queue.size();
    if (a.length < length) {
      finalArray = (T[]) Array.newInstance(a.getClass().getComponentType(), length);
    }
    else {
      finalArray = a;
    }

    Iterator<StableWrapper<E>> iterator = queue.iterator();
    for (int i = 0; i < length; i++) {
      if (iterator.hasNext()) {
        finalArray[i] = (T) iterator.next().object;
      }
      else {
        if (finalArray != a) {
          finalArray = Arrays.copyOf(finalArray, i);
        }
        else {
          finalArray[i] = null;
        }
      }
    }

    return finalArray;
  }

  @Override
  public boolean remove(Object o)
  {
    for (StableWrapper<E> e : queue) {
      if (e.object == o) {
        if (size() == 1) {
          counter = 0;
        }

        return queue.remove(e);
      }
    }

    return false;
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean addAll(Collection<? extends E> c)
  {
    if (c == null) {
      return queue.addAll(null);
    }

    if (c == this) {
      return queue.addAll(queue);
    }

    boolean modified = false;
    for (E e : c) {
      if (add(e)) {
        modified = true;
      }
    }

    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    boolean modified = false;
    if (c == this) {
      if (size() > 0) {
        clear();
        modified = true;
      }
      else {
        modified = false;
      }
      counter = 0;
    }
    else if (c != null) {
      for (Object o : c) {
        if (remove(o)) {
          modified = true;
        }
      }
      
      if (modified && isEmpty()) {
        counter = 0;
      }
    }

    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    ArrayList<StableWrapper<E>> removeThese = new ArrayList<StableWrapper<E>>();
    for (StableWrapper<E> swe : queue) {
      if (!c.contains(swe.object)) {
        removeThese.add(swe);
      }
    }

    if (removeThese.isEmpty()) {
      return false;
    }
    
    if (queue.size() == removeThese.size()) {
      counter = 0;
    }

    return queue.removeAll(removeThese);
  }

  @Override
  public void clear()
  {
    queue.clear();
    counter = 0;
  }
}
