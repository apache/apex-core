/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.api;

import java.lang.reflect.Array;

/**
 * Abstraction for the processing logic or consumption of a data tuple.
 * Implemented by concrete data ports for their processing behavior or by streams.
 *
 * @param <T>
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public interface Sink<T>
{
  /** Constant <code>NO_SINKS</code> */
  @SuppressWarnings("unchecked")
  public static final Sink<Object>[] NO_SINKS = (Sink<Object>[])Array.newInstance(Sink.class, 0);
  /** Constant <code>BLACKHOLE</code> */
  public static final Sink<Object> BLACKHOLE = new Sink<Object>()
  {
    @Override
    public void put(Object tuple)
    {
    }

    @Override
    public int getCount(boolean reset)
    {
      return 0;
    }

  };

  /**
   * Process the payload which can either be user defined object or Tuple.
   *
   * @param tuple payload to be processed by this sink.
   */
  public void put(T tuple);

  /**
   * Give the count of the tuples processed since the last reset.
   *
   * @param reset reset the count if true.
   * @return the count of tuples processed since the last reset.
   */
  public int getCount(boolean reset);

}
