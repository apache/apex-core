/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.*;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ConsoleOutputStream implements Stream, Sink
{
  private StreamContext context;

  @Override
  public void setup(StreamConfiguration config)
  {
  }

  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  @Override
  public StreamContext getContext()
  {
    return context;
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void doSomething(Tuple t)
  {
    switch (t.getType()) {
      case SIMPLE_DATA:
      case PARTITIONED_DATA:
        System.out.println(t.getObject());
        break;

      default:
        System.out.println(t);
        break;
    }
  }

  @Override
  public void activate()
  {
  }
}
