/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Operator;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.Sink;
import com.malhartech.api.StreamCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UnifierNode extends GenericNode
{
  final Unifier<Object> unifier;

  public UnifierNode(String id, Unifier<Object> unifier)
  {
    super(id, unifier);
    this.unifier = unifier;
  }

  @Override
  public InputPort<Object> getInputPort(String port)
  {
    return new InputPort<Object>()
    {
      @Override
      public Sink<Object> getSink()
      {
        throw new UnsupportedOperationException("Not supported yet.");
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
      public Operator getOperator()
      {
        return unifier;
      }

    };
  }

  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
}
