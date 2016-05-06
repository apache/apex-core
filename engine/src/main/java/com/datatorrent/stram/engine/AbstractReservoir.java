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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.netlet.util.UnsafeBlockingQueue;
import com.datatorrent.stram.tuple.Tuple;

import static java.lang.Thread.sleep;

/**
 * Abstract Sweepable Reservoir implementation. Implements all methods of {@link SweepableReservoir} except
 * {@link SweepableReservoir#sweep}. Classes that extend {@link AbstractReservoir} must implement
 * {@link BlockingQueue} interface.
 *
 * @since 3.4.0
 */
public abstract class AbstractReservoir implements SweepableReservoir, BlockingQueue<Object>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractReservoir.class);
  static final String reservoirClassNameProperty = "com.datatorrent.stram.engine.Reservoir";
  private static final int SPSC_ARRAY_BLOCKING_QUEUE_CAPACITY_THRESHOLD = 64 * 1024;

  /**
   * Reservoir factory. Constructs concrete implementation of {@link AbstractReservoir} based on
   * {@link AbstractReservoir#reservoirClassNameProperty} property.
   * @param id reservoir identifier
   * @param capacity reservoir capacity
   * @return concrete implementation of {@link AbstractReservoir}
   */
  public static AbstractReservoir newReservoir(final String id, final int capacity)
  {
    String reservoirClassName = System.getProperty(reservoirClassNameProperty);
    if (reservoirClassName == null) {
      if (capacity >= SPSC_ARRAY_BLOCKING_QUEUE_CAPACITY_THRESHOLD) {
        return new SpscArrayQueueReservoir(id, capacity);
      } else {
        return new SpscArrayBlockingQueueReservoir(id, capacity);
      }
    } else if (reservoirClassName.equals(SpscArrayQueueReservoir.class.getName())) {
      return new SpscArrayQueueReservoir(id, capacity);
    } else if (reservoirClassName.equals(SpscArrayBlockingQueueReservoir.class.getName())) {
      return new SpscArrayBlockingQueueReservoir(id, capacity);
    } else if (reservoirClassName.equals(CircularBufferReservoir.class.getName())) {
      return new CircularBufferReservoir(id, capacity);
    } else if (reservoirClassName.equals(ArrayBlockingQueueReservoir.class.getName())) {
      return new ArrayBlockingQueueReservoir(id, capacity);
    } else {
      try {
        final Constructor<?> constructor = Class.forName(reservoirClassName).getConstructor(String.class, int.class);
        return (AbstractReservoir)constructor.newInstance(id, capacity);
      } catch (ReflectiveOperationException e) {
        logger.debug("Fail to construct reservoir {}", reservoirClassName, e);
        throw new RuntimeException("Fail to construct reservoir " + reservoirClassName, e);
      }
    }
  }

  private Sink<Object> sink;
  private String id;
  protected int count;

  protected AbstractReservoir(final String id)
  {
    this.id = id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    try {
      return this.sink;
    } finally {
      this.sink = sink;
    }
  }

  /**
   * {@inheritDoc}
   */
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
   * @return allocated reservoir capacity
   */
  public abstract int capacity();

  /**
   * @return reservoir id
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

  protected Sink<Object> getSink()
  {
    return sink;
  }

  @Override
  public String toString()
  {
    return getClass().getName() + '@' + Integer.toHexString(hashCode()) +
      "{sink=" + sink + ", id=" + id + ", count=" + count + '}';
  }

  /**
   * <p>SpscArrayQueueReservoir</p>
   * {@link SweepableReservoir} implementation that extends AbstractReservoir and delegates {@link BlockingQueue}
   * implementation to {@see <a href=http://jctools.github.io/JCTools/>JCTools</a>} SpscArrayQueue.
   */
  private static class SpscArrayQueueReservoir extends AbstractReservoir
  {
    private final int maxSpinMillis = 10;
    private final SpscArrayQueue<Object> queue;

    private SpscArrayQueueReservoir(final String id, final int capacity)
    {
      super(id);
      queue = new SpscArrayQueue<>(capacity);
    }

    @Override
    public Tuple sweep()
    {
      Object o;
      final SpscArrayQueue<Object> queue = this.queue;
      final Sink<Object> sink = getSink();
      while ((o = queue.peek()) != null) {
        if (o instanceof Tuple) {
          return (Tuple)o;
        }
        count++;
        sink.put(queue.poll());
      }
      return null;
    }

    @Override
    public boolean add(Object o)
    {
      return queue.add(o);
    }

    @Override
    public Object remove()
    {
      return queue.remove();
    }

    @Override
    public Object peek()
    {
      return queue.peek();
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      return queue.size();
    }

    @Override
    public int capacity()
    {
      return queue.capacity();
    }

    @Override
    public int drainTo(final Collection<? super Object> container)
    {
      return queue.drain(new MessagePassingQueue.Consumer<Object>()
      {
        @Override
        public void accept(Object o)
        {
          container.add(o);
        }
      });
    }

    @Override
    public boolean offer(Object o)
    {
      return queue.offer(o);
    }

    @Override
    public void put(Object o) throws InterruptedException
    {
      long spinMillis = 0;
      final SpscArrayQueue<Object> queue = this.queue;
      while (!queue.offer(o)) {
        sleep(spinMillis);
        spinMillis = Math.min(maxSpinMillis, spinMillis + 1);
      }
    }

    @Override
    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object take() throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity()
    {
      final SpscArrayQueue<Object> queue = this.queue;
      return queue.capacity() - queue.size();
    }

    @Override
    public boolean remove(Object o)
    {
      return queue.remove(o);
    }

    @Override
    public boolean contains(Object o)
    {
      return queue.contains(o);
    }

    @Override
    public int drainTo(final Collection<? super Object> collection, int maxElements)
    {
      return queue.drain(new MessagePassingQueue.Consumer<Object>()
      {
        @Override
        public void accept(Object o)
        {
          collection.add(o);
        }
      }, maxElements);
    }

    @Override
    public Object poll()
    {
      return queue.poll();
    }

    @Override
    public Object element()
    {
      return queue.element();
    }

    @Override
    public boolean isEmpty()
    {
      return queue.peek() == null;
    }

    @Override
    public Iterator<Object> iterator()
    {
      return queue.iterator();
    }

    @Override
    public Object[] toArray()
    {
      return queue.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
      return queue.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
      return queue.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<?> c)
    {
      return queue.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
      return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
      return queue.retainAll(c);
    }

    @Override
    public int size()
    {
      return queue.size();
    }

    @Override
    public void clear()
    {
      queue.clear();
    }

    protected SpscArrayQueue<Object> getQueue()
    {
      return queue;
    }

  }

  /**
   * <p>SpscArrayBlockingQueueReservoir</p>
   * {@link SweepableReservoir} implementation that extends SpscArrayQueueReservoir and delegates {@link BlockingQueue}
   * implementation to {@see <a href=http://jctools.github.io/JCTools/>JCTools</a>} SpscArrayQueue.
   */
  private static class SpscArrayBlockingQueueReservoir extends SpscArrayQueueReservoir
  {
    private final ReentrantLock lock;
    private final Condition notFull;

    private SpscArrayBlockingQueueReservoir(final String id, final int capacity)
    {
      super(id, capacity);
      lock = new ReentrantLock();
      notFull = lock.newCondition();
    }

    @Override
    public Tuple sweep()
    {
      Object o;
      final ReentrantLock lock = this.lock;
      final SpscArrayQueue<Object> queue = getQueue();
      final Sink<Object> sink = getSink();
      lock.lock();
      try {
        while ((o = queue.peek()) != null) {
          if (o instanceof Tuple) {
            return (Tuple)o;
          }
          count++;
          sink.put(queue.poll());
          notFull.signal();
          if (lock.hasQueuedThreads()) {
            return null;
          }
        }
        return null;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void put(Object o) throws InterruptedException
    {
      final SpscArrayQueue<Object> queue = getQueue();
      if (!queue.offer(o)) {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
          while (!queue.offer(o)) {
            notFull.await();
          }
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public Object remove()
    {
      final SpscArrayQueue<Object> queue = getQueue();
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
        Object o = queue.remove();
        if (o != null) {
          notFull.signal();
        }
        return o;
      } finally {
        lock.unlock();
      }
    }


  }

  /**
   * <p>ArrayBlockingQueueReservoir</p>
   * {@link SweepableReservoir} implementation that extends AbstractReservoir and delegates {@link BlockingQueue}
   * implementation to {@link ArrayBlockingQueue}.
   */
  private static class ArrayBlockingQueueReservoir extends AbstractReservoir
  {
    private final ArrayBlockingQueue<Object> queue;

    private ArrayBlockingQueueReservoir(final String id, final int capacity)
    {
      super(id);
      queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public Tuple sweep()
    {
      Object o;
      final ArrayBlockingQueue<Object> queue = this.queue;
      final Sink<Object> sink = getSink();
      while ((o = queue.peek()) != null) {
        if (o instanceof Tuple) {
          return (Tuple)o;
        }
        count++;
        sink.put(queue.poll());
      }
      return null;
    }

    @Override
    public boolean add(Object o)
    {
      return queue.add(o);
    }

    @Override
    public boolean offer(Object o)
    {
      return queue.offer(o);
    }

    @Override
    public void put(Object o) throws InterruptedException
    {
      queue.put(o);
    }

    @Override
    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException
    {
      return queue.offer(o, timeout, unit);
    }

    @Override
    public Object poll()
    {
      return queue.poll();
    }

    @Override
    public Object take() throws InterruptedException
    {
      return queue.take();
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      return queue.poll(timeout, unit);
    }

    @Override
    public Object peek()
    {
      return queue.peek();
    }

    @Override
    public int size()
    {
      return queue.size();
    }

    @Override
    public int size(final boolean dataTupleAware)
    {
      return queue.size();
    }

    @Override
    public int capacity()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity()
    {
      return queue.remainingCapacity();
    }

    @Override
    public boolean remove(Object o)
    {
      return queue.remove(o);
    }

    @Override
    public boolean contains(Object o)
    {
      return queue.contains(o);
    }

    @Override
    public Object[] toArray()
    {
      return queue.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
      return queue.toArray(a);
    }

    @Override
    public String toString()
    {
      return queue.toString();
    }

    @Override
    public void clear()
    {
      queue.clear();
    }

    @Override
    public int drainTo(Collection<? super Object> c)
    {
      return queue.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super Object> c, int maxElements)
    {
      return queue.drainTo(c, maxElements);
    }

    @Override
    public Iterator<Object> iterator()
    {
      return queue.iterator();
    }

    @Override
    public Object remove()
    {
      return queue.remove();
    }

    @Override
    public Object element()
    {
      return queue.element();
    }

    @Override
    public boolean addAll(Collection<?> c)
    {
      return queue.addAll(c);
    }

    @Override
    public boolean isEmpty()
    {
      return queue.isEmpty();
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
      return queue.containsAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
      return queue.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
      return queue.retainAll(c);
    }
  }

  /**
   * <p>CircularBufferReservoir</p>
   * {@link SweepableReservoir} implementation that extends AbstractReservoir and delegates {@link BlockingQueue}
   * implementation to {@code CircularBuffer}. Replaces DefaultReservoir class since release 3.3}.
   *
   * @since 0.3.2
   */
  private static class CircularBufferReservoir extends AbstractReservoir implements UnsafeBlockingQueue<Object>
  {
    private final CircularBuffer<Object> circularBuffer;

    private CircularBufferReservoir(String id, int capacity)
    {
      super(id);
      circularBuffer = new CircularBuffer<>(capacity);
    }

    @Override
    public Tuple sweep()
    {
      final CircularBuffer<Object> circularBuffer = this.circularBuffer;
      final Sink<Object> sink = getSink();
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
    public boolean add(Object o)
    {
      return circularBuffer.add(o);
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

    @Override
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
    public boolean offer(Object o)
    {
      return circularBuffer.offer(o);
    }

    @Override
    public void put(Object o) throws InterruptedException
    {
      circularBuffer.put(o);
    }

    @Override
    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException
    {
      return circularBuffer.offer(o, timeout, unit);
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
    public int size()
    {
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
  }

}


