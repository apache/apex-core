/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.stram.StramChild;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
/**
 *
 * Implements a stream that is read from a socket by a node<p>
 * <br>
 * The main class for all socket based input streams.<br>
 * <br>
 *
 * @param <T>
 */
public abstract class SocketInputStream<T> extends VarIntLengthPrependerClient implements Stream<T>
{
  @Override
  public void setup(StreamContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(StreamContext context)
  {
    StramChild.eventloop.connect(context.getBufferServerAddress(), this);
  }

  @Override
  public void deactivate()
  {
    StramChild.eventloop.disconnect(this);
  }

  private static final Logger logger = LoggerFactory.getLogger(SocketInputStream.class);
}
