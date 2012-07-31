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
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {
    // nothing to do?
  }

  @Override
  public StreamContext getContext()
  {
    return this.context;
  }

  @Override
  public void activate()
  {
  }
}
