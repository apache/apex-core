/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UnifierNode extends GenericNode
{
  final Unifier<Object> unifier;

  class UnifiedPort implements InputPort<Object>, Sink<Object>
  {
    private int count;

    @Override
    public Sink<Object> getSink()
    {
      return this;
    }

    @Override
    public void setConnected(boolean connected)
    {
    }

    @Override
    public Class<? extends StreamCodec<Object>> getStreamCodec()
    {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public final void put(Object tuple)
    {
      count++;
      unifier.process(tuple);
    }

    @Override
    public int getCount(boolean reset)
    {
      try {
      return count;
      }
      finally {
        if (reset) {
          count = 0;
        }
      }
    }

    @Override
    public void setup(PortContext context)
    {
    }

    @Override
    public void teardown()
    {
    }
  }

  final UnifiedPort unifiedPort = new UnifiedPort();

  public UnifierNode(Unifier<Object> unifier)
  {
    super(unifier);
    this.unifier = unifier;
  }

  @Override
  public InputPort<Object> getInputPort(String port)
  {
    descriptor.inputPorts.put(port, unifiedPort);
    return unifiedPort;
  }

  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
}
