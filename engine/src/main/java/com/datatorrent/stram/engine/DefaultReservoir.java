/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.stram.engine;


import com.datatorrent.api.Sink;

import com.datatorrent.netlet.util.CircularBuffer;
import com.datatorrent.stram.tuple.Tuple;

/**
 * <p>DefaultReservoir class.</p>
 *
 * @since 0.3.2
 */
public class DefaultReservoir extends CircularBuffer<Object> implements SweepableReservoir
{
  private Sink<Object> sink;
  private String id;
  private int count;

  public DefaultReservoir(String id, int capacity)
  {
    super(capacity);
    this.id = id;
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
    final int size = size();
    for (int i = 0; i < size; i++) {
      if (peekUnsafe() instanceof Tuple) {
        count += i;
        return (Tuple)peekUnsafe();
      }
      sink.put(pollUnsafe());
    }

    count += size;
    return null;
  }

  @Override
  public String toString()
  {
    return "DefaultReservoir{" + "sink=" + sink + ", id=" + id + ", count=" + count + '}';
  }

  /**
   * @return the id
   */
  public String getId()
  {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id)
  {
    this.id = id;
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

}
