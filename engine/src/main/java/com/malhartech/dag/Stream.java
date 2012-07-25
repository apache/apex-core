/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public interface Stream extends DAGPart<StreamConfiguration, StreamContext>
{
  @Override
  public void setup(StreamConfiguration config);

  public void setContext(StreamContext context);

  public StreamContext getContext();

  public void activate();
  
  @Override
  public void teardown();
}
