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

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;

import org.jctools.queues.SpscArrayQueue;

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

import static com.datatorrent.bufferserver.packet.MessageType.BEGIN_WINDOW;

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
  private static final int CAPACITY = Integer.getInteger(capacityPropertyName, 1 << 19);

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
    assertEquals(sink, reservoir.sink);
  }

  @SuppressWarnings("unused")
  private Object defaultTestParameters()
  {
    Object[][] defaultTestParameters = new Object[][] {
        {null, NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayQueueReservoir", NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$ArrayBlockingQueueReservoir", NoSuchElementException.class},
        {"com.datatorrent.stram.engine.AbstractReservoir$CircularBufferReservoir", IllegalStateException.class}
    };

    for (Object[] o: defaultTestParameters) {
      o[0] = newReservoir((String)o[0], 2);
      final Sink<Object> sink = new Sink<Object>()
      {
        final ArrayList<Object> sink = new ArrayList<>();
        int count;
        @Override
        public void put(Object tuple)
        {
          sink.add(tuple);
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
        {null, 500},
        {"com.datatorrent.stram.engine.AbstractReservoir$SpscArrayQueueReservoir", 500},
        {"com.datatorrent.stram.engine.AbstractReservoir$ArrayBlockingQueueReservoir", 6000},
        {"com.datatorrent.stram.engine.AbstractReservoir$CircularBufferReservoir", 2000}
    };
    for (Object[] o : performanceTestParameters) {
      o[0] = newReservoir((String)o[0], CAPACITY);
      final Sink<Object> sink = new Sink<Object>()
      {
        private int count = 0;
        @Override
        public void put(Object tuple)
        {
          count++;
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
    assertEquals(1, reservoir.sink.getCount(false));
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
    assertEquals(0, reservoir.sink.getCount(false));
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
  @Parameters(method = "performanceTestParameters")
  public void performanceTest(final AbstractReservoir reservoir, final long expectedTime)
  {
    int maxQueueSize = 0;

    final long start = System.currentTimeMillis();

    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        final Object o = new Byte[128];
        try {
          for (int i = 0; i < COUNT; i++) {
            reservoir.put(o);
          }
        } catch (InterruptedException e) {
          logger.debug("Interrupted", e);
        }
      }
    }).start();

    while (reservoir.sink.getCount(false) < COUNT) {
      maxQueueSize = Math.max(maxQueueSize, reservoir.size(false));
      reservoir.sweep();
    }

    long time = System.currentTimeMillis() - start;
    logger.debug("{}: time {}, max queue size {}", reservoir.getId(), time, maxQueueSize);
    assertTrue("Expected to complete within " + expectedTime + "millis. Actual time " + time + " milis",
        expectedTime > time);

  }

  @Test
  public void testBlockingQueuePerformance()
  {
    final ArrayBlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>(CAPACITY);
    long start = System.currentTimeMillis();
    new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            final Object o = new Byte[128];
            try {
              for (int i = 0; i < COUNT; i++) {
                blockingQueue.put(o);
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
    ).start();

    try {
      for (int i = 0; i < COUNT; i++) {
        blockingQueue.take();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

  @Test
  public void testSpscQueuePerformance()
  {
    final SpscArrayQueue<Object> spscArrayQueue = new SpscArrayQueue<>(CAPACITY);
    long start = System.currentTimeMillis();
    new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            final Object o = new Byte[128];
            try {
              for (int i = 0; i < COUNT; i++) {
                while (!spscArrayQueue.offer(o)) {
                  Thread.sleep(10);
                }
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
    ).start();

    try {
      for (int i = 0; i < COUNT; i++) {
        while (spscArrayQueue.poll() == null) {
          Thread.sleep(10);
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

  @Test
  public void testCircularBufferPerformance()
  {
    final CircularBuffer<Object> circularBuffer = new CircularBuffer<>(CAPACITY);
    long start = System.currentTimeMillis();
    new Thread(
        new Runnable()
        {
          @Override
          public void run()
          {
            final Object o = new Byte[128];
            try {
              for (int i = 0; i < COUNT; i++) {
                circularBuffer.put(o);
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
    ).start();

    try {
      for (int i = 0; i < COUNT; i++) {
        circularBuffer.take();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    logger.debug("Time {}", System.currentTimeMillis() - start);
  }

}
