/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.engine;

import com.datatorrent.tuple.EndStreamTuple;
import com.datatorrent.tuple.Tuple;
import com.datatorrent.api.Sink;
import com.datatorrent.bufferserver.packet.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T>
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class WindowIdActivatedReservoir implements SweepableReservoir
{
  private Sink<Object> sink;
  private final String identifier;
  private final SweepableReservoir reservoir;
  private final long windowId;
  EndStreamTuple est;

  public WindowIdActivatedReservoir(String identifier, SweepableReservoir reservoir, final long windowId)
  {
    this.identifier = identifier;
    this.reservoir = reservoir;
    this.windowId = windowId;

    reservoir.setSink(Sink.BLACKHOLE);
  }

  @Override
  public int size()
  {
    return reservoir.size();
  }

  @Override
  public Object remove()
  {
    if (est == null) {
      return reservoir.remove();
    }

    try {
      return est;
    }
    finally {
      est = null;
    }
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
    Tuple t;
    while ((t = reservoir.sweep()) != null) {
      if (t.getType() == MessageType.BEGIN_WINDOW && t.getWindowId() > windowId) {
        reservoir.setSink(sink);
        return (est = new EndStreamTuple(windowId));
      }
      reservoir.remove();
    }

    return null;
  }

  @Override
  public int getCount(boolean reset)
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "WindowIdActivatedReservoir{" + "identifier=" + identifier + ", windowId=" + windowId + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(WindowIdActivatedReservoir.class);
}
