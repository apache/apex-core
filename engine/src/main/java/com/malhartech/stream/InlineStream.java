/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.Tuple;

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

  @Override
  public void setup(StreamConfiguration config)
  {
    // nothing to do?
  }

  @Override
  public void setContext(com.malhartech.dag.StreamContext context)
  {
    this.context = context;
  }

  @Override
  public void teardown()
  {
    // nothing to do?
  }

}
