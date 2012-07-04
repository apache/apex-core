/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.stram.StreamContext;

/**
 *
 * @author chetan
 */
public class InlineStream implements Sink, Stream
{
  private com.malhartech.dag.StreamContext context;
  public void doSomething(Tuple t)
  {
    t.setContext(context);
    context.getSink().doSomething(t);
  }

  public void setup(StreamConfiguration config)
  {
    // nothing to do?
  }

  public void process(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
  }

  public void teardown(StreamConfiguration config)
  {
    // nothing to do?
  }

}
