/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.dag.Sink;
import com.malhartech.dag.Stream;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class InlineStream implements Stream
{
  Sink current, output, shunted = new Sink()
  {
    @Override
    public void process(Object payload)
    {
    }
  };

  @Override
  public void setup(StreamConfiguration config)
  {
    // nothing to be done here.
  }

  @Override
  public void activate(StreamContext context)
  {
    current = output;
  }

  @Override
  public void deactivate()
  {
    current = shunted;
  }

  @Override
  public void teardown()
  {
    current = null;
  }

  @Override
  public Sink connect(String port, Sink sink)
  {
    if (INPUT.equals(port)) {
      return this;
    }
    else {
      output = sink;
    }
    return null;
  }

  @Override
  public final void process(Object payload)
  {
    current.process(payload);
  }
}
