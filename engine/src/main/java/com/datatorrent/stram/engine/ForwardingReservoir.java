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

import com.datatorrent.api.Sink;
import com.datatorrent.stram.tuple.Tuple;

/**
 * @since 3.4.0
 */
public class ForwardingReservoir implements SweepableReservoir
{
  public static ForwardingReservoir newReservoir(final String id, final int capacity)
  {
    return new ForwardingReservoir(AbstractReservoir.newReservoir(id, capacity));
  }

  private final AbstractReservoir reservoir;

  public ForwardingReservoir(AbstractReservoir reservoir)
  {
    this.reservoir = reservoir;
  }

  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    return reservoir.setSink(sink);
  }

  @Override
  public int getCount(boolean reset)
  {
    return reservoir.getCount(reset);
  }

  public String getId()
  {
    return reservoir.getId();
  }

  public void setId(String id)
  {
    reservoir.setId(id);
  }

  public boolean add(Object o)
  {
    return reservoir.add(o);
  }

  public void put(Object o) throws InterruptedException
  {
    reservoir.put(o);
  }

  @Override
  public Tuple sweep()
  {
    return reservoir.sweep();
  }

  @Override
  public int size(final boolean dataTupleAware)
  {
    return reservoir.size(dataTupleAware);
  }

  @Override
  public Object remove()
  {
    return reservoir.remove();
  }

  @Override
  public boolean isEmpty()
  {
    return reservoir.isEmpty();
  }

  public AbstractReservoir getReservoir()
  {
    return reservoir;
  }

}
