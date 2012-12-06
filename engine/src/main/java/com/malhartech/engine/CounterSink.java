/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.engine.OperatorStats.Counter;
import java.lang.reflect.Array;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface CounterSink<T> extends Sink<T>, Counter
{
  @SuppressWarnings( {"unchecked", "FieldNameHidesFieldInSuperclass"})
  public static final CounterSink<Object>[] NO_SINKS = (CounterSink<Object>[])Array.newInstance(CounterSink.class, 0);
}
