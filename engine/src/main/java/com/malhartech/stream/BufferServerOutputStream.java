/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferServerOutputStream extends SocketOutputStream
{
  private static Logger LOG = LoggerFactory.getLogger(BufferServerOutputStream.class);

  public void setContext(StreamContext context, String sourceId, String streamLogicalName)
  {
    super.setContext(context);

    LOG.info("registering publisher: {} {}", sourceId, streamLogicalName);
    ClientHandler.publish(channel, sourceId, streamLogicalName, context.getWindowId());
  }
  
  @Override
  public void setContext(StreamContext context) {
    throw new UnsupportedOperationException("setContext requires additional parameters.");
  }
  
}