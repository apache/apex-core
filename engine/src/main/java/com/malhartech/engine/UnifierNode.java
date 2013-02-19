/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Context.PortContext;
import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.Sink;
import com.malhartech.util.AttributeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class UnifierNode extends GenericNode
{
  private static final Logger logger = LoggerFactory.getLogger(UnifierNode.class);
  final Unifier<Object> unifier;

  private class MergeReservoir extends AbstractReservoir
  {
    MergeReservoir(String portname, int bufferSize, int spinMillis)
    {
      super(portname, bufferSize, spinMillis);
    }

    @Override
    public final Tuple sweep()
    {
      int size = size();
      for (int i = 1; i <= size; i++) {
        if (peekUnsafe() instanceof Tuple) {
          count += i;
          return (Tuple)peekUnsafe();
        }

        unifier.merge(pollUnsafe());
      }

      count += size;
      return null;
    }

  }

  public UnifierNode(String id, Unifier<Object> unifier)
  {
    super(id, unifier);
    this.unifier = unifier;
  }

  @Override
  public Sink<Object> connectInputPort(String port, AttributeMap<PortContext> attributes, Sink<? extends Object> sink)
  {
    MergeReservoir retvalue;

    if (sink == null) {
      Reservoir reservoir = inputs.remove(port);
      if (reservoir != null) {
        inputs.put(port.concat(".").concat(String.valueOf(deletionId++)), reservoir);
        reservoir.process(new EndStreamTuple());
      }

      retvalue = null;
    }
    else {
      inputs.put(port, retvalue = new MergeReservoir(port,
                                                     attributes == null ? 1024 * 1024 : attributes.attrValue(PortContext.BUFFER_SIZE, 1024 * 1024),
                                                     attributes == null ? 15 : attributes.attrValue(PortContext.SPIN_MILLIS, 15)));
    }

    return retvalue;
  }

}
