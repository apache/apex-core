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
    System.out.print(t.getType() + " window = " + t.getWindowId() + " ");
    switch (t.getType()) {
      case BEGIN_WINDOW:
        break;
        
      case END_WINDOW:
        System.out.print("tuples = " + ((EndWindowTuple) t).getTupleCount());
        break;

      case PARTITIONED_DATA:
        System.out.print("partitioned ");
        
      case SIMPLE_DATA:
        System.out.print(t.getObject() + " ");
        break;

      default:
        System.out.print("how come?!!");
        break;
    }
    
    System.out.println();
  }

  public void activate()
  {
  }
}
