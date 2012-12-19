/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/**
 *
 * Inline streams are used for performance enhancement when both the operators are in the same hadoop container<p>
 * <br>
 * Inline is a hint that the stram can choose to ignore. Stram may also convert a normal stream into an inline one
 * for performance reasons. A stream tagged with persist flag will not be inlined, as persistence requires a buffer
 * server<br>
 * Inline streams currently cannot be partitioned. Since the main reason for partitioning
 * is to load balance and that means across different hadoop containers. In future we may take a look at it.<br>
 * <br>
 *
 */
public class InlineStream implements Stream<Object>
{
  private volatile Sink<Object> current, output, shunted = new Sink<Object>()
  {
    @Override
    public void process(Object payload)
    {
    }
  };

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    // nothing to be done here.
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
    current = output;
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    current = shunted;
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
    current = null;
  }

  /**
   *
   * @param port
   * @param sink
   */
  @Override
  public void setSink(String port, Sink<Object> sink)
  {
    if (current == output) {
      current = sink;
    }
    output = sink;
  }

  /**
   *
   * @param payload
   */
  @Override
  public final void process(Object payload)
  {
    current.process(payload);
  }

  public final Sink<Object> getOutput()
  {
    return output;
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return false;
  }
}
