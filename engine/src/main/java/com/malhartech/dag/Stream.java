/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.dag;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
/*
 * Provides basic interface for a stream object. Stram, StramChild work via this interface
 */
public interface Stream extends DAGPart<StreamConfiguration, StreamContext>
{
  public static final StreamContext DISCONNECTED_STREAM_CONTEXT = new StreamContext(null, null)
  {
    @Override
    public void sink(Tuple t)
    {
    }
  };

  @Override
  public void setup(StreamConfiguration config);

  public void activate(StreamContext context);

  @Override
  public void teardown();
}
