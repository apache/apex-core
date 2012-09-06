/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 *
 * @author chetan
 */

public class MuxStream implements Stream
{
  HashMap<String, Sink> outputs;
  Collection<Sink> sinks = Collections.EMPTY_LIST;

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
  public void activate(StreamContext context)
  {
    sinks = outputs.values();
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    sinks.clear();
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
    for (Sink s: sinks) {
      s.process(payload);
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }
}
