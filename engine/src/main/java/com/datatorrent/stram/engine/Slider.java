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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StatsListener;

/**
 * <p>Slider class.</p>
 *
 * @since 3.2.0
 */
public class Slider implements Unifier<Object>, Operator.IdleTimeHandler, Operator.ActivationListener<OperatorContext>, StatsListener, Serializable, Operator.CheckpointListener
{
  private List<List<Object>> cache;
  private transient List<Object> currentList;
  private final Unifier<Object> unifier;
  private final int numberOfBuckets;
  private final int numberOfSlideBuckets;
  private transient int spinMillis;
  public final transient DefaultOutputPort<Object> outputPort = new DefaultOutputPort<>();
  private transient int cacheSize;

  public Unifier<Object> getUnifier()
  {
    return unifier;
  }

  private Slider()
  {
    unifier = null;
    numberOfBuckets = -1;
    numberOfSlideBuckets = -1;
  }

  public Slider(Unifier<Object> uniOperator, int buckets, int numberOfSlideBuckets)
  {
    unifier = uniOperator;
    cache = new LinkedList<>();
    this.numberOfBuckets = buckets;
    this.numberOfSlideBuckets = numberOfSlideBuckets;
  }

  private OutputPort<?> getOutputPort()
  {
    for (Class<?> c = unifier.getClass(); c != Object.class; c = c.getSuperclass()) {
      Field[] fields = c.getDeclaredFields();
      for (Field field : fields) {
        field.setAccessible(true);
        try {
          Object portObject = field.get(unifier);
          if (portObject instanceof OutputPort) {
            return (OutputPort<?>)portObject;
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException("Unifier should have exactly one output port");
  }

  @Override
  public void process(Object tuple)
  {
    if (cacheSize == numberOfBuckets - 1) {
      unifier.process(tuple);
    }
    currentList.add(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    cacheSize = cache.size();
    unifier.beginWindow(windowId);
    if (cacheSize == numberOfBuckets - 1) {
      for (List<Object> windowCache : cache) {
        for (Object obj : windowCache) {
          unifier.process(obj);
        }
      }
    }
    currentList = new LinkedList<>();
  }

  @Override
  public void endWindow()
  {
    cache.add(currentList);
    if (cacheSize == numberOfBuckets - 1) {
      for (int i = 0; i < numberOfSlideBuckets; i++) {
        cache.remove(0);
      }
    }
    unifier.endWindow();
  }

  @Override
  public void setup(OperatorContext context)
  {
    OutputPort<?> unifierOutputPort = getOutputPort();
    unifierOutputPort.setSink(
        new Sink<Object>()
        {
          @Override
          public void put(Object tuple)
          {
            outputPort.emit(tuple);
          }

          @Override
          public int getCount(boolean reset)
          {
            return 0;
          }
        }
    );
    unifier.setup(context);
    spinMillis = context.getValue(OperatorContext.SPIN_MILLIS);
  }

  @Override
  public void teardown()
  {
    unifier.teardown();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void activate(OperatorContext context)
  {
    if (unifier instanceof ActivationListener) {
      ((ActivationListener<OperatorContext>)unifier).activate(context);
    }
  }

  @Override
  public void deactivate()
  {
    if (unifier instanceof ActivationListener) {
      ((ActivationListener)unifier).deactivate();
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (unifier instanceof IdleTimeHandler) {
      ((IdleTimeHandler)unifier).handleIdleTime();
    } else {
      try {
        Thread.sleep(spinMillis);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    if (unifier instanceof StatsListener) {
      return ((StatsListener)unifier).processStats(stats);
    }
    return null;
  }

  @Override
  public void checkpointed(long windowId)
  {
    if (unifier instanceof CheckpointListener) {
      ((CheckpointListener)unifier).checkpointed(windowId);
    }
  }

  @Override
  public void committed(long windowId)
  {
    if (unifier instanceof CheckpointListener) {
      ((CheckpointListener)unifier).committed(windowId);
    }
  }

  private static final long serialVersionUID = 201505251917L;
}
