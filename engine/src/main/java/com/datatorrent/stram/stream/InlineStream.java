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

import com.datatorrent.stram.engine.AbstractReservoir;
import com.datatorrent.stram.engine.ForwardingReservoir;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;

/**
 *
 * When data exchange is needed between 2 operators deployed in the same container, they are connected using a
 * blocking queue; The implementation of such a blocking queue is InlineStream.<br />
 *
 * @since 0.3.2
 */
public class InlineStream extends ForwardingReservoir implements Stream, SweepableReservoir
{
  public InlineStream(int capacity)
  {
    super(AbstractReservoir.newReservoir("InlineStream", capacity));
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    setId(context.getId());
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
      super.put(tuple);
    }
    catch (InterruptedException ie) {
      logger.debug("Interrupted", ie);
      throw new RuntimeException(ie);
    }
  }

  @Override
  public String toString()
  {
    return getClass().getName() + '@' + Integer.toHexString(hashCode()) + "{reservoir=" + getReservoir().toString() + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(InlineStream.class);
}
