/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.api;

import java.lang.reflect.Array;

/**
 * Abstraction for the processing logic or consumption of a data tuple.
 * Implemented by concrete data ports for their processing behavior or by streams.
 *
 * @param <T>
 */
public interface Sink<T>
{
  @SuppressWarnings("unchecked")
  public static final Sink<Object>[] NO_SINKS = (Sink<Object>[])Array.newInstance(Sink.class, 0);
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