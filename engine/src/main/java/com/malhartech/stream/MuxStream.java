/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 *
 * @author chetan
 */

public class MuxStream implements Stream
{
  HashMap<String, Sink> outputs;
  Sink[] sinks = new Sink[0];

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
    sinks = new Sink[outputs.size()];

    int i = 0;
    for (final Sink s: outputs.values()) {
      sinks[i++] = s;
    }
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    sinks = new Sink[0];
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
    for (int i = sinks.length; i-- > 0;) {
      try {
        sinks[i].process(payload);
      }
      catch (MutatedSinkException mse) {
        Sink newSink = mse.getNewSink();
        newSink.process(payload);
        sinks[i] = newSink;

        Sink oldSink = mse.getOldSink();
        for (Entry<String, Sink> e: outputs.entrySet()) {
          if (e.getValue() == oldSink) {
            outputs.put(e.getKey(), oldSink);
            break;
          }
        }
      }
    }
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }
}
