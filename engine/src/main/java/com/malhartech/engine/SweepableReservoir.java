/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Reservoir /* extends UnsafeBlockingQueue<Object>, Sink<Object> */

{
  public void setSink(Sink<Object> sink);

  public Tuple sweep();

  /**
   * the count of elements in this Reservoir.
   *
   * @return the count
   */
  public int size();

  /**
   * Remove the element from head/tail?
   */
  public Object remove();

}
