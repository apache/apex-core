/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.engine;

import com.malhartech.api.Sink;
import com.malhartech.util.CircularBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractReservoir extends CircularBuffer<Object> implements Reservoir, Sink<Object>
{
  protected int count;
  final String portname;

  AbstractReservoir(String portname, int bufferCapacity, int spinMillis)
  {
    super(bufferCapacity, spinMillis);
    this.portname = portname;
  }

  @Override
  public final void process(Object payload)
  {
    try {
      put(payload);
    }
    catch (InterruptedException ex) {
      logger.warn("Abandoning processing of the payload {} due to an interrupt", payload);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public String toString()
  {
    return portname;
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractReservoir.class);
}
