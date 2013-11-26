/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.engine.DefaultReservoir;
import com.datatorrent.stram.engine.Stream;
import com.datatorrent.stram.engine.StreamContext;
import com.datatorrent.stram.engine.SweepableReservoir;

/**
 *
 * When data exchange is needed between 2 operators deployed in the same container, they are connected using a
 * blocking queue; The implementation of such a blocking queue is InlineStream.<br />
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class InlineStream extends DefaultReservoir implements Stream, SweepableReservoir
{
  public InlineStream(int capacity)
  {
    super("InlineStream", capacity);
  }

  /**
   *
   * @param context
   */
  @Override
  public void setup(StreamContext context)
  {
    setId(context.getId());
  }

  /**
   *
   * @param context
   */
  @Override
  public void activate(StreamContext context)
  {
  }

  /**
   *
   */
  @Override
  public void deactivate()
  {
    clear();
  }

  /**
   *
   */
  @Override
  public void teardown()
  {
    clear();
  }

  @Override
  public void put(Object tuple)
  {
    try {
      super.put(tuple);
    }
    catch (InterruptedException ie) {
      logger.debug("Interrupted", ie);
      throw new RuntimeException(ie);
    }
  }

  @Override
  public String toString()
  {
    return "InlineStream{" + super.toString() + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(InlineStream.class);
}
