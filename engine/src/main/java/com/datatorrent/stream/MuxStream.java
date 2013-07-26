/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stream;

import com.datatorrent.engine.Stream;
import com.datatorrent.engine.StreamContext;
import com.datatorrent.api.Sink;
import java.lang.reflect.Array;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>MuxStream class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class MuxStream implements Stream
{
  private HashMap<String, Sink<Object>> outputs = new HashMap<String, Sink<Object>>();
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink<Object>[] sinks = NO_SINKS;
  private int count;

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
    outputs.clear();
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
  public void put(Object payload)
  {
    count++;
    for (int i = sinks.length; i-- > 0;) {
      sinks[i].put(payload);
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
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

  private static final Logger logger = LoggerFactory.getLogger(MuxStream.class);
}
