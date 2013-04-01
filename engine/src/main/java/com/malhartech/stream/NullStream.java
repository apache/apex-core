/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.api.Sink;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class NullStream implements Stream<Object>
{

  @Override
  public boolean isMultiSinkCapable()
  {
    return true;
  }

  @Override
  public void setSink(String sinkId, Sink<Object> sink)
  {
  }

  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext ctx)
  {
  }

  @Override
  public void deactivate()
  {
  }

  @Override
  public void process(Object tuple)
  {
  }
}
