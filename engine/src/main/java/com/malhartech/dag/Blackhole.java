/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

/**
 * 
 * To send tuples to no where<p>
 * <br>
 * Used with an library node has an optional output port is not connected<br>
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Blackhole implements Sink {

  @Override
  public void doSomething(Tuple t)
  {
    t.setContext(null);
  }

}
