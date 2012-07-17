/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.StreamContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferServerOutputSocketStream extends OutputSocketStream
{
  private static Logger LOG = LoggerFactory.getLogger(BufferServerOutputSocketStream.class);

  public void setContext(StreamContext context, String upstreamNodeId, String streamLogicalName)
  {
    super.setContext(context);

    LOG.info("registering publisher: {} {}", upstreamNodeId, streamLogicalName);
    ClientHandler.publish(channel, upstreamNodeId, streamLogicalName, context.getWindowId());
  }
  
  @Override
  public void setContext(StreamContext context) {
    throw new UnsupportedOperationException("setContext requires additional parameters.");
  }
  
}