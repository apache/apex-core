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

  public void setup(StreamConfiguration config)
  {
  }

  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  public StreamContext getContext()
  {
    return context;
  }

  public void teardown()
  {
  }

  public void doSomething(Tuple t)
  {
    System.out.println(t);
  }

  public void activate()
  {
  }
}
