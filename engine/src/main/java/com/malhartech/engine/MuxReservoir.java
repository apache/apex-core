/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.tuple.Tuple;
import com.malhartech.util.CircularBuffer;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class MuxReservoir
{
  @SuppressWarnings("VolatileArrayField")
  private volatile SubReservoir[] reservoirs = new SubReservoir[0];
  private HashMap<String, SubReservoir> reservoirMap = new HashMap<String, SubReservoir>();

  public SweepableReservoir acquireReservoir(String id, int capacity)
  {
    SubReservoir r = reservoirMap.get(id);
    if (r == null) {
      reservoirMap.put(id, r = new SubReservoir(capacity));
      SubReservoir[] newReservoirs = new SubReservoir[reservoirs.length + 1];
      newReservoirs[reservoirs.length] = r;
      for (int i = reservoirs.length; i-- > 0;) {
        newReservoirs[i] = reservoirs[i];
      }
      reservoirs = newReservoirs;
    }

    return r;
  }

  public SweepableReservoir releaseReservoir(String id)
  {
    SubReservoir r = reservoirMap.remove(id);
    if (r != null) {
      SubReservoir[] newReservoirs = new SubReservoir[reservoirs.length - 1];

      int j = 0;
      for (int i = 0; i < reservoirs.length; i++) {
        if (reservoirs[i] != r) {
          newReservoirs[j++] = reservoirs[i];
        }
      }

      reservoirs = newReservoirs;
    }

    return r;
  }

  public abstract Reservoir getMasterReservoir();

  class SubReservoir extends CircularBuffer<Object> implements SweepableReservoir
  {
    int count;
    private Sink<Object> sink;

    SubReservoir(int capacity)
    {
      super(capacity);
    }

    @Override
    public Sink<Object> setSink(Sink<Object> sink)
    {
      try {
        return this.sink;
      }
      finally {
        this.sink = sink;
      }
    }

    @Override
    public Tuple sweep()
    {
      final int size = size();
      if (size > 0) {
        for (int i = 1; i <= size; i++) {
          if (peekUnsafe() instanceof Tuple) {
            count += i;
            return (Tuple)peekUnsafe();
          }
          sink.put(pollUnsafe());
        }

        count += size;
      }

      final Reservoir masterReservoir = getMasterReservoir();
      synchronized (masterReservoir) {
        /* find out the minimum remaining capacity in all the other buffers and consume those many tuples from bufferserver */
        int min = masterReservoir.size();
        if (min == 0) {
          return null;
        }

        for (int i = reservoirs.length; i-- > 0;) {
          if (reservoirs[i].remainingCapacity() < min) {
            min = reservoirs[i].remainingCapacity();
          }
        }

        while (min-- > 0) {
          Object o = masterReservoir.remove();
          for (int i = reservoirs.length; i-- > 0;) {
            reservoirs[i].add(o);
          }
        }
      }

      return null;
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

  }

  private static final Logger logger = LoggerFactory.getLogger(MuxReservoir.class);
}
