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

import com.datatorrent.api.Sink;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;
import com.datatorrent.stram.tuple.Tuple;

/**
 * A non buffering stream which facilitates the ThreadLocal implementation of an operator.
 *
 * @since 0.3.5
 */
public class OiOStream implements Stream, SweepableReservoir
{
  private Sink<Object> sink;
  private Sink<Tuple> control;
  private int count;

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  public void setControlSink(Sink<Tuple> control)
  {
    this.control = control;
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
    }
    else {
      count++;
      sink.put(t);
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    try {
      return count;
    }
    finally {
      if (reset) {
        count = 0;
      }
    }
  }

  @Override
  public Sink<Object> setSink(Sink<Object> sink)
  {
    try {
      return this.sink;
    }
    finally {
      this.sink = sink;
    }
  }

  @Override
  public Tuple sweep()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  /**
   * OiOStream is active when there is exactly one tuple present.
   * It's an error to have more than one tuple active on OiO.
   */
  @Override
  public int size(final boolean dataTupleAware)
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
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
