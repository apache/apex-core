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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.stram.tuple.EndStreamTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>WindowIdActivatedReservoir class.</p>
 *
 * @since 0.3.2
 */
public class WindowIdActivatedReservoir implements SweepableReservoir
{
  private Sink<Object> sink;
  private final String identifier;
  private final SweepableReservoir reservoir;
  private final long windowId;
  EndStreamTuple est;

  public WindowIdActivatedReservoir(String identifier, SweepableReservoir reservoir, final long windowId)
  {
    this.identifier = identifier;
    this.reservoir = reservoir;
    this.windowId = windowId;

    reservoir.setSink(Sink.BLACKHOLE);
  }

  @Override
  public int size(final boolean dataTupleAware)
  {
    return reservoir.size(dataTupleAware);
  }

  @Override
  public boolean isEmpty()
  {
    return reservoir.isEmpty();
  }

  @Override
  public Object remove()
  {
    if (est == null) {
      return reservoir.remove();
    }

    try {
      return est;
    } finally {
      est = null;
    }
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
  public Tuple sweep()
  {
    Tuple t;
    while ((t = reservoir.sweep()) != null) {
      if (t.getType() == MessageType.BEGIN_WINDOW && t.getWindowId() > windowId) {
        reservoir.setSink(sink);
        return (est = new EndStreamTuple(windowId));
      }
      reservoir.remove();
    }

    return null;
  }

  @Override
  public int getCount(boolean reset)
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "WindowIdActivatedReservoir{" + "identifier=" + identifier + ", windowId=" + windowId + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(WindowIdActivatedReservoir.class);
}
