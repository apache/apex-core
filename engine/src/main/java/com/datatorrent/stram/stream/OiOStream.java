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

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.tuple.CustomControlTuple;
import com.datatorrent.stram.tuple.Tuple;

/**
 * A non buffering stream which facilitates the ThreadLocal implementation of an operator.</p>
 *
 * {@link OiOStream} can not implement both {@link Stream} and {@link SweepableReservoir} interfaces as
 * {@code getCount(boolean)} should reset the proper count. In case, OioStream implements both {@link Stream} and
 * {@link SweepableReservoir}, it will not be possible to distinguish which count should be reset to 0, so either
 * {@link Sink#getCount(boolean)} or {@link SweepableReservoir#getCount(boolean)} would return a wrong result: let's say
 * upstream operator puts n tuples into {@link OiOStream}, both {@link Sink#getCount(boolean) Sink.getCount(true)} and
 * {@link SweepableReservoir#getCount(boolean) SweepableReservoir.getCount(true)} should return n and reset count to 0.
 * If there is only one count, only one call (whichever comes first) would return n, the second one would return 0.
 *
 * @since 0.3.5
 */
public class OiOStream implements Stream
{
  private Sink<Object> sink;
  private Sink<Tuple> control;
  private int count;
  private OiOReservoir reservoir = new OiOReservoir();

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext cntxt)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void put(Object t)
  {
    if (t instanceof Tuple) {
      control.put((Tuple)t);
    } else {
      count++;
      reservoir.count++;
      sink.put(t);
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

  public SweepableReservoir getReservoir()
  {
    return reservoir;
  }

  public class OiOReservoir implements SweepableReservoir
  {
    private int count;

    public void setControlSink(Sink<Tuple> control)
    {
      OiOStream.this.control = control;
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
    public Sink<Object> setSink(Sink<Object> sink)
    {
      try {
        return OiOStream.this.sink;
      } finally {
        OiOStream.this.sink = sink;
      }
    }

    public Sink<Object> getSink()
    {
      return OiOStream.this.sink;
    }

    @Override
    public Tuple sweep()
    {
      throw new UnsupportedOperationException("Not supported.");
    }

    /**
     * OiOStream is active when there is exactly one tuple present.
     * It's an error to have more than one tuple active on OiO.
     */
    @Override
    public int size(boolean dataTupleAware)
    {
      return 1;
    }

    @Override
    public boolean isEmpty()
    {
      return false;
    }

    @Override
    public Object remove()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
