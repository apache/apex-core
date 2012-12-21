/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import java.lang.reflect.Array;
import java.util.HashMap;

/**
 *
 * @author chetan
 */
public class MuxStream implements Stream<Object>
{
  private HashMap<String, Sink<Object>> outputs;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    outputs = new HashMap<String, Sink<Object>>();
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
    outputs.clear();
    outputs = null;
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    @SuppressWarnings("unchecked")
    Sink<Object>[] newSinks = (Sink<Object>[])Array.newInstance(Sink.class, outputs.size());

    int i = 0;
    for (final Sink<Object> s: outputs.values()) {
      newSinks[i++] = s;
    }
    sinks = newSinks;
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    sinks = NO_SINKS;
  }

  /**
   *
   * @param id
   * @param sink
   */
  @Override
  public void setSink(String id, Sink<Object> sink)
  {
    if (sink == null) {
      outputs.remove(id);
      if (outputs.isEmpty()) {
        sinks = NO_SINKS;
      }
    }
    else {
      outputs.put(id, sink);
      if (sinks != NO_SINKS) {
        activate(null);
      }
    }
  }

  /**
   *
   * @param payload
   */
  @Override
  public void process(Object payload)
  {
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].process(payload);
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

}
