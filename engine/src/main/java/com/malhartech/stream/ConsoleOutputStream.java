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

/**
 * 
 * Writes tuples to stdout of the container<p>
 * <br>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 *
 */
public class ConsoleOutputStream implements Stream, Sink
{
  private StreamContext context;

  /**
   * 
   * @param config 
   */
  @Override
  public void setup(StreamConfiguration config)
  {
  }

  /**
   * 
   * @param context 
   */
  @Override
  public void setContext(StreamContext context)
  {
    this.context = context;
  }

  /**
   * 
   * @return {@link com.malhartech.dag.StreamContext}
   */
  @Override
  public StreamContext getContext()
  {
    return context;
  }

  /**
   * 
   */
  @Override
  public void teardown()
  {
  }

  /**
   * 
   * @param t 
   */
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

  /**
   * 
   */
  @Override
  public void activate()
  {
  }
}
