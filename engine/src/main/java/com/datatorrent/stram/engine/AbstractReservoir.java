/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.engine;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.netlet.util.UnsafeBlockingQueue;
import com.datatorrent.stram.tuple.Tuple;

public abstract class AbstractReservoir implements SweepableReservoir, BlockingQueue<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractReservoir.class);

  public static AbstractReservoir newReservoir(final String id, final int capacity)
  {
    return new DefaultReservoir(id, capacity);
  }

  protected Sink<Object> sink;
  private String id;
  protected int count;

  protected AbstractReservoir(final String id)
  {
    this.id = id;
  }

  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    try {
      return this.sink;
    } finally {
      this.sink = sink;
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    } finally {
      if (reset) {
        count = 0;
      }
    }
  }

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
  }

  @Override
  public String toString()
  {
    return getClass().getName() + '@' + Integer.toHexString(hashCode()) +
      "{sink=" + sink + ", id=" + id + ", count=" + count + '}';
  }

  /**
   * <p>DefaultReservoir class.</p>
   *
   * @since 0.3.2
   */
  private static class DefaultReservoir extends AbstractReservoir implements UnsafeBlockingQueue<Object>
  {
    private final CircularBuffer<Object> circularBuffer;

    DefaultReservoir(String id, int capacity)
    {
      super(id);
      circularBuffer = new CircularBuffer<>(capacity);
    }

    @Override
    public Tuple sweep()
    {
      final int size = circularBuffer.size();
      for (int i = 0; i < size; i++) {
        if (circularBuffer.peekUnsafe() instanceof Tuple) {
          count += i;
          return (Tuple)peekUnsafe();
        }
        sink.put(pollUnsafe());
      }

      count += size;
      return null;
    }

    @Override
    public boolean add(Object e)
    {
      return circularBuffer.add(e);
    }

    @Override
    public Object remove()
    {
      return circularBuffer.remove();
    }

    @Override
    public Object peek()
    {
      return circularBuffer.peek();
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      int size = circularBuffer.size();
      if (dataTupleAware) {
        Iterator<Object> iterator = circularBuffer.getFrozenIterator();
        while (iterator.hasNext()) {
          if (iterator.next() instanceof Tuple) {
            size--;
          }
        }
      }
      return size;
    }

    public int capacity()
    {
      return circularBuffer.capacity();
    }

    @Override
    public int drainTo(Collection<? super Object> container)
    {
      return circularBuffer.drainTo(container);
    }

    @Override
    public boolean offer(Object e)
    {
      return circularBuffer.offer(e);
    }

    @Override
    public void put(Object e) throws InterruptedException
    {
      circularBuffer.put(e);
    }

    @Override
    public boolean offer(Object e, long timeout, TimeUnit unit) throws InterruptedException
    {
      return circularBuffer.offer(e, timeout, unit);
    }

    @Override
    public Object take() throws InterruptedException
    {
      return circularBuffer.take();
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      return circularBuffer.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity()
    {
      return circularBuffer.remainingCapacity();
    }

    @Override
    public boolean remove(Object o)
    {
      return circularBuffer.remove(o);
    }

    @Override
    public boolean contains(Object o)
    {
      return circularBuffer.contains(o);
    }

    @Override
    public int drainTo(Collection<? super Object> collection, int maxElements)
    {
      return circularBuffer.drainTo(collection, maxElements);
    }

    @Override
    public Object poll()
    {
      return circularBuffer.poll();
    }

    @Override
    public Object pollUnsafe()
    {
      return circularBuffer.pollUnsafe();
    }

    @Override
    public Object element()
    {
      return circularBuffer.element();
    }

    @Override
    public boolean isEmpty()
    {
      return circularBuffer.isEmpty();
    }

    public Iterator<Object> getFrozenIterator()
    {
      return circularBuffer.getFrozenIterator();
    }

    public Iterable<Object> getFrozenIterable()
    {
      return circularBuffer.getFrozenIterable();
    }

    @Override
    public Iterator<Object> iterator()
    {
      return circularBuffer.iterator();
    }

    @Override
    public Object[] toArray()
    {
      return circularBuffer.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
      return circularBuffer.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
      return circularBuffer.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<?> c)
    {
      return circularBuffer.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
      return circularBuffer.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
      return circularBuffer.retainAll(c);
    }

    @Override
    public int size() {
      return circularBuffer.size();
    }

    @Override
    public void clear()
    {
      circularBuffer.clear();
    }

    @Override
    public Object peekUnsafe()
    {
      return circularBuffer.peekUnsafe();
    }

    public CircularBuffer<Object> getWhitehole(String exceptionMessage)
    {
      return circularBuffer.getWhitehole(exceptionMessage);
    }

    @Override
    public boolean equals(Object o)
    {
      return circularBuffer.equals(o);
    }

    @Override
    public int hashCode()
    {
      return circularBuffer.hashCode();
    }

  }

}


