/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

import com.malhartech.stram.StreamContext;

/**
 *
 * @author chetan
 */
public interface Sink
{
  public StreamContext getStreamContext();
  public void doSomething(Tuple t);
}
