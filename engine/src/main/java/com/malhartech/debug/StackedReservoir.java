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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object peek()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int size()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isEmpty()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<Object> iterator()
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
  public boolean addAll(Collection<? extends Object> c)
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

}
