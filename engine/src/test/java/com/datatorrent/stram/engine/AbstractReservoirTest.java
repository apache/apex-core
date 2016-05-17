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

import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jctools.queues.SpscArrayQueue;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.tuple.Tuple;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import static com.datatorrent.api.Context.PortContext.QUEUE_CAPACITY;
import static com.datatorrent.api.Context.PortContext.SPIN_MILLIS;
import static com.datatorrent.bufferserver.packet.MessageType.BEGIN_WINDOW;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class AbstractReservoirTest
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractReservoirTest.class);

  private static final String countPropertyName = "com.datatorrent.stram.engine.AbstractReservoirTest.count";
  private static final String capacityPropertyName = "com.datatorrent.stram.engine.AbstractReservoirTest.capacity";
  private static final int COUNT = Integer.getInteger(countPropertyName, 10000000);
  private static final int CAPACITY = Integer.getInteger(capacityPropertyName, QUEUE_CAPACITY.defaultValue);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static AbstractReservoir newReservoir(final String reservoirClassName, final int capacity)
  {
    if (reservoirClassName == null) {
      System.clearProperty(AbstractReservoir.reservoirClassNameProperty);
    } else {
      System.setProperty(AbstractReservoir.reservoirClassNameProperty, reservoirClassName);
    }
    final String id = reservoirClassName == null ? "DefaultReservoir" : reservoirClassName;
    final AbstractReservoir reservoir = AbstractReservoir.newReservoir(id, capacity);
    return reservoir;
  }

  private static void setSink(final AbstractReservoir reservoir, final Sink<Object> sink)
  {
    assertNull(reservoir.setSink(sink));
    assertEquals(sink, reservoir.getSink());
  }

  @SuppressWarnings("unused")
  private Object defaultTestParameters()
  {
    Object[][] defaultTestParameters = new Object[][] {
        {null, NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayQueueReservoir", NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayBlockingQueueReservoir", NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$ArrayBlockingQueueReservoir", NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$CircularBufferReservoir", IllegalStateException.class}
    };

    for (Object[] o: defaultTestParameters) {
      o[0] = newReservoir((String)o[0], 2);
      final Sink<Object> sink = new Sink<Object>()
      {
        private int count;
        @Override
        public void put(Object tuple)
        {
          count++;
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
      };
      setSink((AbstractReservoir)o[0], sink);
    }
    return defaultTestParameters;
  }

  @SuppressWarnings("unused")
  private Object performanceTestParameters()
  {
    Object[][] performanceTestParameters = new Object[][] {
        {null, 2500},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayQueueReservoir", 10000},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayBlockingQueueReservoir", 2500},
        {"com.datatorrent.stram.engine.AbstractReservoir$ArrayBlockingQueueReservoir", 10000},
        {"com.datatorrent.stram.engine.AbstractReservoir$CircularBufferReservoir", 100000}
    };
    for (Object[] o : performanceTestParameters) {
      o[0] = newReservoir((String)o[0], CAPACITY);
      final Sink<Object> sink = new Sink<Object>()
      {
        private int count = 0;
        @Override
        public void put(Object tuple)
        {
          if (++count == COUNT) {
            throw new RuntimeException();
          }
        }

        @Override
        public int getCount(boolean reset)
        {
          return count;
        }
      };
      setSink((AbstractReservoir)o[0], sink);
    }
    return performanceTestParameters;
  }

  @Test
  @Parameters(method = "defaultTestParameters")
  public void testEmpty(final AbstractReservoir reservoir, final Class<? extends Throwable> type)
  {
    assertTrue(reservoir.isEmpty());
    assertEquals(0, reservoir.size());
    assertEquals(0, reservoir.size(true));
    assertEquals(0, reservoir.size(false));
    assertNull(reservoir.sweep());
    exception.expect(type);
    reservoir.remove();
  }

  @Test
  @Parameters(method = "defaultTestParameters")
  public void testAddAndSweepObject(final AbstractReservoir reservoir, final Class<? extends Throwable> type)
  {
    final Object o = new Integer(0);
    assertTrue(reservoir.add(o));
    assertFalse(reservoir.isEmpty());
    assertEquals(1, reservoir.size());
    assertEquals(1, reservoir.size(false));
    assertEquals(0, reservoir.getCount(false));
    assertEquals(o, reservoir.peek());
    assertNull(reservoir.sweep());
    assertEquals(1, reservoir.getCount(false));
    assertEquals(1, reservoir.getSink().getCount(false));
    assertTrue(reservoir.isEmpty());
    assertEquals(0, reservoir.size());
    assertEquals(0, reservoir.size(false));
    exception.expect(type);
    reservoir.remove();
  }

  @Test
  @Parameters(method = "defaultTestParameters")
  public void testAddAndSweepTuple(final AbstractReservoir reservoir, final Class<? extends Throwable> type)
  {
    final Tuple t = new Tuple(BEGIN_WINDOW, 0L);
    assertTrue(reservoir.add(t));
    assertFalse(reservoir.isEmpty());
    assertEquals(1, reservoir.size());
    assertEquals(1, reservoir.size(false));
    assertEquals(t, reservoir.peek());
    assertEquals(t, reservoir.sweep());
    assertEquals(t, reservoir.sweep());
    assertEquals(0, reservoir.getCount(false));
    assertEquals(0, reservoir.getSink().getCount(false));
    assertFalse(reservoir.isEmpty());
    assertEquals(t, reservoir.remove());
    assertNull(reservoir.peek());
    assertNull(reservoir.poll());
    assertNull(reservoir.sweep());
    exception.expect(type);
    reservoir.remove();
  }

  @Test
  @Parameters(method = "defaultTestParameters")
  public void testFullReservoir(final AbstractReservoir reservoir, final Class<? extends Throwable> type)
  {
    int capacity = reservoir.remainingCapacity();
    assertTrue(capacity > 0);
    final Object o = new Integer(0);
    for (int i = 0; i < capacity; i++) {
      assertTrue(reservoir.offer(o));
    }
    assertFalse(reservoir.offer(o));
    exception.expect(IllegalStateException.class);
    reservoir.add(o);
  }

  @Test
  @Ignore
  @Parameters(method = "performanceTestParameters")
  public void performanceTest(final AbstractReservoir reservoir, final long expectedTime)
  {
    final long start = System.currentTimeMillis();
    final int maxSleepTime = SPIN_MILLIS.defaultValue;

    final Thread t = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              int sleepMillis;
              while (true) {
                sleepMillis = 0;
                reservoir.sweep();
                while (reservoir.isEmpty()) {
                  sleep(Math.min(maxSleepTime, sleepMillis++));
                }
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted", e);
              throw new RuntimeException(e);
            } catch (RuntimeException e) {
              assertEquals(COUNT, reservoir.getSink().getCount(false));
            }
          }
        }
    );

    t.start();

    final Object o = new Byte[128];
    try {
      for (int i = 0; i < COUNT; i++) {
        reservoir.put(o);
      }
      t.join();
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      throw new RuntimeException(e);
    }

    long time = System.currentTimeMillis() - start;
    logger.debug("{}: time {}", reservoir.getId(), time);
    assertTrue(reservoir.getId() + ": expected to complete within " + expectedTime + " millis. Actual time "
        + time + " millis", expectedTime > time);

  }

  @Test
  @Ignore
  public void testBlockingQueuePerformance()
  {
    final ArrayBlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(CAPACITY);
    final long start = System.currentTimeMillis();
    final Thread t = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              for (int i = 0; i < COUNT; i++) {
                blockingQueue.take();
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted", e);
              throw new RuntimeException(e);
            }
          }
        }
    );

    t.start();

    final Object o = new Byte[128];
    try {
      for (int i = 0; i < COUNT; i++) {
        blockingQueue.put(o);
      }
      t.join();
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      throw new RuntimeException(e);
    }

    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

  @Test
  @Ignore
  public void testSpscQueuePerformance()
  {
    final SpscArrayQueue<Object> spscArrayQueue = new SpscArrayQueue<>(CAPACITY);
    final long start = System.currentTimeMillis();
    final Thread t = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              int sleepMillis;
              for (int i = 0; i < COUNT; i++) {
                sleepMillis = 0;
                while (spscArrayQueue.poll() == null) {
                  sleep(sleepMillis);
                  sleepMillis = Math.min(10, sleepMillis + 1);
                }
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted", e);
              throw new RuntimeException(e);
            }
          }
        }
    );

    t.start();

    final Object o = new Byte[128];
    int sleepMillis;
    try {
      for (int i = 0; i < COUNT; i++) {
        sleepMillis = 0;
        while (!spscArrayQueue.offer(o)) {
          sleep(sleepMillis);
          sleepMillis = Math.min(10, sleepMillis + 1);
        }
      }
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      throw new RuntimeException(e);
    }

    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

  @Test
  @Ignore
  public void testSpscBlockingQueuePerformance()
  {
    final SpscArrayQueue<Object> spscArrayQueue = new SpscArrayQueue<>(CAPACITY);
    final ReentrantLock lock = new ReentrantLock();
    final Condition notFull = lock.newCondition();
    final long start = System.currentTimeMillis();
    final Thread t = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              int sleepMillis;
              for (int i = 0; i < COUNT; i++) {
                sleepMillis = 0;
                while (spscArrayQueue.poll() == null) {
                  sleep(sleepMillis);
                  sleepMillis = Math.min(10, sleepMillis + 1);
                }
                lock.lock();
                notFull.signal();
                lock.unlock();
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted", e);
              throw new RuntimeException(e);
            }
          }
        }
    );

    t.start();

    final Object o = new Byte[128];
    try {
      for (int i = 0; i < COUNT; i++) {
        if (!spscArrayQueue.offer(o)) {
          lock.lockInterruptibly();
          while (!spscArrayQueue.offer(o)) {
            notFull.await();
          }
          lock.unlock();
        }
      }
      t.join();
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      throw new RuntimeException(e);
    }

    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

  @Test
  @Ignore
  public void testCircularBufferPerformance()
  {
    final CircularBuffer<Object> circularBuffer = new CircularBuffer<>(CAPACITY);
    final long start = System.currentTimeMillis();
    final Thread t = new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              int sleepMillis;
              for (int i = 0; i < COUNT; i++) {
                sleepMillis = 0;
                while (circularBuffer.poll() == null) {
                  sleep(sleepMillis);
                  sleepMillis = Math.min(10, sleepMillis + 1);
                }
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted", e);
              throw new RuntimeException(e);
            }
          }
        }
    );

    t.start();

    final Object o = new Byte[128];
    try {
      for (int i = 0; i < COUNT; i++) {
        circularBuffer.put(o);
      }
      t.join();
    } catch (InterruptedException e) {
      logger.error("Interrupted", e);
      throw new RuntimeException(e);
    }

    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

}
