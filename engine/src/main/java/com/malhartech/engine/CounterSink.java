/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.engine.OperatorStats.Counter;
import java.lang.reflect.Array;

/**
 * CounterSink counts the objects which are sunk into it.
 * @param <T> - Type of objects which can be sunk into this sink.
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface CounterSink<T> extends Sink<T>, Counter
{
  @SuppressWarnings( {"unchecked", "FieldNameHidesFieldInSuperclass"})
  public static final CounterSink<Object>[] NO_SINKS = (CounterSink<Object>[])Array.newInstance(CounterSink.class, 0);
}
