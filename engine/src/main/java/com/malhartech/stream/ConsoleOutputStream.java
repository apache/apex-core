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
public class ConsoleOutputStream implements Stream
{
  @Override
  public void setup(StreamConfiguration config)
  {
  }

  @Override
  public void teardown()
  {
  }

  /**
   *
   * @param t the value of t
   */
  @Override
  public void process(Object t)
  {
    System.out.println(t);
  }

  @Override
  public void activate(StreamContext context)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public Sink connect(String id, DAGComponent component)
  {
    return this;
  }
}
