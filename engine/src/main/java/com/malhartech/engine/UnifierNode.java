/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Operator.Unifier;
import com.malhartech.api.Sink;
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

  private class MergeReservoir extends Reservoir
  {

    MergeReservoir(String portname)
    {
      super(portname);
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
  public Sink<Object> connectInputPort(String port, Sink<? extends Object> sink)
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
      inputs.put(port, retvalue = new MergeReservoir(port));
    }

    return retvalue;
  }

}
