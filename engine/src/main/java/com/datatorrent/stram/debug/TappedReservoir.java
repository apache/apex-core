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
package com.datatorrent.stram.debug;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>TappedReservoir class.</p>
 *
 * @since 0.3.2
 */
public class TappedReservoir extends MuxSink implements SweepableReservoir
{
  public final SweepableReservoir reservoir;
  private Sink<Object> sink;

  @SuppressWarnings({"unchecked", "LeakingThisInConstructor"})
  public TappedReservoir(SweepableReservoir reservoir, Sink<Object> tap)
  {
    this.reservoir = reservoir;
    add(tap);
    sink = reservoir.setSink(this);
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
    return reservoir.sweep();
  }

  @Override
  public int getCount(boolean reset)
  {
    return reservoir.getCount(reset);
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
  public void put(Object tuple)
  {
    super.put(tuple);
    sink.put(tuple);
  }

  @Override
  public Object remove()
  {
    Object object = reservoir.remove();
    super.put(object);
    return object;
  }

}
