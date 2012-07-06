/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.malhartech.dag;

/**
 * Just for fun!
 * 
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Blackhole implements Sink {

  public void doSomething(Tuple t)
  {
    t.setContext(null);
    t.setData(null);
    t.setContext(null);
  }

}
