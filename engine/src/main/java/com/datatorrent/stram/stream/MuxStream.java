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

import java.lang.reflect.Array;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.tuple.CustomControlTuple;

/**
 * <p>MuxStream class.</p>
 *
 * @since 0.3.2
 */
public class MuxStream implements Stream.MultiSinkCapableStream
{
  public static final String MULTI_SINK_ID_CONCAT_SEPARATOR = ", ";
  private HashMap<String, Sink<Object>> outputs = new HashMap<>();
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;
  private int count;

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
    outputs.clear();
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    @SuppressWarnings("unchecked")
    Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, outputs.size());

    int i = 0;
    for (final Sink<Object> s: outputs.values()) {
      newSinks[i++] = s;
    }
    sinks = newSinks;
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    sinks = NO_SINKS;
  }

  /**
   *
   * @param id
   * @param sink
   */
  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    if (sink == null) {
      outputs.remove(id);
      if (outputs.isEmpty()) {
        sinks = NO_SINKS;
      }
    } else {
      outputs.put(id, sink);
      if (sinks != NO_SINKS) {
        activate(null);
      }
    }
  }

  /**
   *
   * @param payload
   */
  @Override
  public void put(Object payload)
  {
    count++;
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].put(payload);
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

  private static final Logger logger = LoggerFactory.getLogger(MuxStream.class);
}
