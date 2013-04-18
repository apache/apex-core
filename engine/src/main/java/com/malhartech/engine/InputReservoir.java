/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
class InputReservoir extends AbstractReservoir
{
  final Sink<Object> sink;

  InputReservoir(Sink<Object> sink, String portname, int bufferSize, int spinMillis)
  {
    super(portname, bufferSize, spinMillis);
    this.sink = sink;
  }

  @Override
  public Tuple sweep()
  {
    final int size = size();
    for (int i = 1; i <= size; i++) {
      if (peekUnsafe() instanceof Tuple) {
        count += i;
        return (Tuple)peekUnsafe();
      }
      sink.process(pollUnsafe());
    }

    count += size;
    return null;
  }

  @Override
  public void consume(Object payload)
  {
    sink.process(payload);
  }

}
