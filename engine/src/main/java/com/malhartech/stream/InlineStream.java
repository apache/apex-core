/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamContext;

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
  private volatile Sink current, output, shunted = new Sink()
  {
    @Override
    public void process(Object payload)
    {
    }
  };

  /**
   *
   * @param config
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
  public void postActivate(StreamContext context)
  {
    current = output;
  }

  /**
   *
   */
  @Override
  public void preDeactivate()
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
   * @return Sink
   */
  @Override
  public Sink setSink(String port, Sink sink)
  {
    Sink previous;
    if (current == output) {
      current = sink;
    }
    previous = output;
    output = sink;
    return previous;
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

  public final Sink getOutput()
  {
    return output;
  }

  @Override
  public boolean isMultiSinkCapable()
  {
    return false;
  }
}
