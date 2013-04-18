/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.Sink;
import com.malhartech.stream.BufferServerSubscriber;
import com.malhartech.tuple.EndStreamTuple;
import com.malhartech.util.AttributeMap;
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
  public Sink<Object> connectInputPort(String port, AttributeMap<PortContext> attributes, Sink<? extends Object> sink)
  {
    AbstractReservoir retvalue;

    if (sink == null) {
      Reservoir reservoir = inputs.remove(port);
      if (reservoir != null) {
        inputs.put(port.concat(".").concat(String.valueOf(deletionId++)), reservoir);
        reservoir.process(new EndStreamTuple());
      }

      retvalue = null;
    }
    else {
      int bufferCapacity = attributes == null ? 16 * 1024 : attributes.attrValue(PortContext.BUFFER_SIZE, 16 * 1024);
      int spinMilliseconds = attributes == null ? 15 : attributes.attrValue(PortContext.SPIN_MILLIS, 15);
      if (sink instanceof BufferServerSubscriber) {
        retvalue = new BufferServerReservoir(unifier, port, bufferCapacity, spinMilliseconds, ((BufferServerSubscriber)sink).getSerde(), ((BufferServerSubscriber)sink).getBaseSeconds());
      }
      else {
        retvalue = new InputReservoir(unifier, port, bufferCapacity, spinMilliseconds);
      }
      inputs.put(port, retvalue);
    }

    return retvalue;
  }

  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
}
