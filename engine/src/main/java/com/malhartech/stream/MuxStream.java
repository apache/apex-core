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
/**
 *
 * Inline streams are used for performance enhancement when both the nodes are in the same hadoop container<p>
 * <br>
 * Inline is a hint that the stram can choose to ignore. Stram may also convert a normal stream into an inline one
 * for performance reasons. A stream tagged with persist flag will not be inlined, as persistence requires a buffer
 * server<br>
 * Inline streams currently cannot be partitioned. Since the main reason for partitioning
 * is to load balance and that means across different hadoop containers. In future we may take a look at it.<br>
 * <br>
 *
 */
public class MuxStream implements Stream
{
  HashMap<String, Sink> outputs;
  Collection<Sink> sinks = Collections.EMPTY_LIST;

  @Override
  public void setup(StreamConfiguration config)
  {
    outputs = new HashMap<String, Sink>();
  }

  @Override
  public void teardown()
  {
    outputs.clear();
    outputs = null;
  }

  @Override
  public void activate(StreamContext context)
  {
    sinks = outputs.values();
  }

  @Override
  public void deactivate()
  {
    sinks.clear();
  }

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

  @Override
  public void process(Object payload)
  {
    for (Sink s: sinks) {
      s.process(payload);
    }
  }
}
