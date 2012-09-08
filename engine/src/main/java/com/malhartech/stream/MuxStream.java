/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.HashMap;

/**
 *
 * @author chetan
 */

public class MuxStream implements Stream
{
  private HashMap<String, Sink> outputs;
  @SuppressWarnings("VolatileArrayField")
  private volatile Sink[] sinks = NO_SINKS;

  /**
   *
   * @param config
   */
  @Override
  public void setup(StreamConfiguration config)
  {
    outputs = new HashMap<String, Sink>();
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
  @SuppressWarnings("SillyAssignment")
  public void activate(StreamContext context)
  {
    sinks = new Sink[outputs.size()];

    int i = 0;
    for (final Sink s: outputs.values()) {
      sinks[i++] = s;
    }
    sinks = sinks;
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
   * @return Sink
   */
  @Override
  public Sink connect(String id, Sink sink)
  {
    if (INPUT.equals(id)) {
      return this;
    }
    else if (sink == null) {
      outputs.remove(id);
    }
    else {
      outputs.put(id, sink);
      if (sinks != NO_SINKS) {
        activate(null);
      }
    }

    return null;
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
