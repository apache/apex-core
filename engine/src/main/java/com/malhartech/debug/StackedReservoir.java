/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.debug;

import com.malhartech.api.Sink;
import com.malhartech.engine.Reservoir;
import com.malhartech.engine.Tuple;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class StackedReservoir implements Reservoir
{
  Reservoir reservoir;
  Sink<Object> stackedSink;

  StackedReservoir(Reservoir original, Sink<Object> sink)
  {
    reservoir = original;
    stackedSink = sink;
  }

  @Override
  public Object remove()
  {
    Object o = reservoir.remove();
    stackedSink.process(o);
    return o;
  }

  @Override
  public Object pollUnsafe()
  {
    Object o = reservoir.pollUnsafe();
    stackedSink.process(o);
    return o;
  }

  @Override
  public Tuple sweep()
  {
    Tuple t = reservoir.sweep();
    return t;
  }

  @Override
  public Object peekUnsafe()
  {
    return reservoir.peekUnsafe();
  }

  @Override
  public boolean add(Object e)
  {
    return reservoir.add(e);
  }

  @Override
  public boolean offer(Object e)
  {
    return reservoir.offer(e);
  }

  @Override
  public void put(Object e) throws InterruptedException
  {
    reservoir.put(e);
  }

  @Override
  public boolean offer(Object e, long timeout, TimeUnit unit) throws InterruptedException
  {
    return reservoir.offer(e, timeout, unit);
  }

  @Override
  public Object take() throws InterruptedException
  {
    Object o = reservoir.take();
    stackedSink.process(o);
    return o;
  }

  @Override
  public Object poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    Object o = reservoir.poll(timeout, unit);
    stackedSink.process(o);
    return o;
  }

  @Override
  public int remainingCapacity()
  {
    return reservoir.remainingCapacity();
  }

  @Override
  public boolean remove(Object o)
  {
    if (reservoir.remove(o)) {
      stackedSink.process(o);
      return true;
    }

    return false;
  }

  @Override
  public boolean contains(Object o)
  {
    return reservoir.contains(o);
  }

  @Override
  public int drainTo(Collection<? super Object> c)
  {
    return reservoir.drainTo(c);
  }

  @Override
  public int drainTo(Collection<? super Object> c, int maxElements)
  {
    return reservoir.drainTo(c, maxElements);
  }

  @Override
  public Object poll()
  {
    Object o = reservoir.poll();
    stackedSink.process(o);
    return o;
  }

  @Override
  public Object element()
  {
    return reservoir.element();
  }

  @Override
  public Object peek()
  {
    return reservoir.peek();
  }

  @Override
  public int size()
  {
    return reservoir.size();
  }

  @Override
  public boolean isEmpty()
  {
    return reservoir.isEmpty();
  }

  @Override
  public Iterator<Object> iterator()
  {
    return reservoir.iterator();
  }

  @Override
  public Object[] toArray()
  {
    return reservoir.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a)
  {
    return reservoir.toArray(a);
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    return reservoir.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends Object> c)
  {
    return reservoir.addAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    boolean retValue = false;
    Iterator<Object> iterator = reservoir.iterator();

    while (iterator.hasNext()) {
      Object o = iterator.next();
      if (c.contains(o)) {
        stackedSink.process(o);
        iterator.remove();
        retValue = true;
      }
    }
    return retValue;
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    boolean retValue = false;
    Iterator<Object> iterator = reservoir.iterator();

    while (iterator.hasNext()) {
      Object o = iterator.next();
      if (!c.contains(o)) {
        stackedSink.process(o);
        iterator.remove();
        retValue = true;
      }
    }
    return retValue;
  }

  @Override
  public void clear()
  {
    while (!isEmpty()) {
      this.poll();
    }
  }

}
