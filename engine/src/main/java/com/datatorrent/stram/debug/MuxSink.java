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

import java.lang.reflect.Array;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Sink;

/**
 * <p>MuxSink class.</p>
 *
 * @since 0.3.2
 */
public class MuxSink implements Sink<Object>
{
  Sink<Object>[] sinks;
  private int count;

  public MuxSink(Sink<Object>... s)
  {
    sinks = s;
  }

  @SuppressWarnings("unchecked")
  public MuxSink()
  {
    sinks = (Sink<Object>[])Array.newInstance(Sink.class, 0);
  }

  @Override
  public void put(Object tuple)
  {
    count++;
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].put(tuple);
    }
  }

  public void add(Sink<Object>... s)
  {
    int i = sinks.length;
    sinks = Arrays.copyOf(sinks, i + s.length);
    for (Sink<Object> ss : s) {
      sinks[i++] = ss;
    }
  }

  public void remove(Sink<Object> s)
  {
    boolean found = false;
    for (int i = sinks.length; i-- > 0;) {
      if (sinks[i] == s) {
        sinks[i] = null;
        found = true;
      }
    }

    if (found) {
      @SuppressWarnings("unchecked")
      Sink<Object>[] newInstance = (Sink<Object>[])Array.newInstance(Sink.class, sinks.length - 1);
      int i = 0;
      for (int j = sinks.length; j-- > 0;) {
        if (sinks[j] != null) {
          newInstance[i++] = sinks[j];
        }
      }

      sinks = newInstance;
    }
  }

  public void remove(Sink<Object>... s)
  {
    /* mark all the sinks to be deleted as null */
    int found = 0;
    for (int i = s.length; i-- > 0;) {
      for (int j = sinks.length; j-- > 0;) {
        if (s[i] == sinks[j]) {
          sinks[j] = null;
          found++;
          break;
        }
      }
    }

    /* copy over rest of the sinks to a new array */
    @SuppressWarnings({"unchecked"})
    Sink<Object>[] newInstance = (Sink<Object>[])Array.newInstance(Sink.class, sinks.length - found);
    int i = 0;
    for (int j = sinks.length; j-- > 0;) {
      if (sinks[j] != null) {
        newInstance[i++] = sinks[j];
      }
    }

    /* now new array is our final list of sinks */
    sinks = newInstance;
  }

  /**
   * Get the count of sinks supported currently.
   *
   * @return the count of sinks catered.
   */
  public Sink<Object>[] getSinks()
  {
    return Arrays.copyOf(sinks, sinks.length);
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

  private static final Logger logger = LoggerFactory.getLogger(MuxSink.class);
}
