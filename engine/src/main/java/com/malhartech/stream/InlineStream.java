/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;

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
public class InlineStream implements Sink, Stream
{
  private StreamContext context;

  @Override
  public void doSomething(Tuple t)
  {
    context.sink(t);
  }

  @Override
  public void setup(StreamConfiguration config)
  {
    // nothing to do?
  }

  @Override
  public void teardown()
  {
    // nothing to do?
  }

  @Override
  public void activate(StreamContext context)
  {
  }
}
