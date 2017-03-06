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
package com.datatorrent.stram.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.stram.engine.AbstractReservoir;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.tuple.CustomControlTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 *
 * When data exchange is needed between 2 operators deployed in the same container, they are connected using a
 * blocking queue; The implementation of such a blocking queue is InlineStream.<br />
 *
 * @since 0.3.2
 */
public class InlineStream implements Stream
{
  private int count;
  private AbstractReservoir reservoir;

  public InlineStream(int capacity)
  {
    reservoir = AbstractReservoir.newReservoir("InlineStream", capacity);
  }

  public SweepableReservoir getReservoir()
  {
    return reservoir;
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    reservoir.setId(context.getId());
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Object tuple)
  {
    try {
      reservoir.put(tuple);
      if (!(tuple instanceof Tuple)) {
        count++;
      }
    } catch (InterruptedException ie) {
      logger.debug("Interrupted", ie);
      throw new RuntimeException(ie);
    }
  }

  @Override
  public boolean putControl(ControlTuple payload)
  {
    put(new CustomControlTuple(payload));
    return false;
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

  @Override
  public String toString()
  {
    return getClass().getName() + '@' + Integer.toHexString(hashCode()) + "{reservoir=" + getReservoir().toString() + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(InlineStream.class);
}
