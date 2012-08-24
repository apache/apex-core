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

  @Override
  public void setup(StreamConfiguration config)
  {
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
  public void activate(StreamContext context)
  {
  }

  @Override
  public void deactivate()
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
