/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.util.UnsafeBlockingQueue;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Reservoir extends UnsafeBlockingQueue<Object>
{
  public abstract Tuple sweep();

}
