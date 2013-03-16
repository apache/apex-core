/*
 * Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.engine.Stream;
import com.malhartech.engine.StreamContext;
import com.malhartech.stram.StramChild;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements Stream class and provides basic stream connection for a node to write to a socket<p>
 * <br>
 * Most likely users would not use it to write to a socket by themselves. Is used in adapters and by BufferServerOutputStream<br>
 * <br>
 *
 * @param <T> 
 * @author chetan
 */
public abstract class SocketOutputStream<T> extends VarIntLengthPrependerClient implements Stream<Object>
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

  private static final Logger logger = LoggerFactory.getLogger(SocketOutputStream.class);
}
