/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.ClientHandler;

public class BufferServerOutputSocketStream extends OutputSocketStream
{
  private static Logger LOG = LoggerFactory.getLogger(BufferServerOutputSocketStream.class);

  public void setContext(StreamContext context, String upstreamNodeId, String streamLogicalName)
  {
    super.setContext(context);
    // send publisher request
    LOG.info("registering publisher: {} {}", upstreamNodeId, streamLogicalName);
    ClientHandler.publish(channel, upstreamNodeId, streamLogicalName);
  }
  
  @Override
  public void setContext(StreamContext context) {
    throw new UnsupportedOperationException("setContext requires additional parameters.");
  }
  
}