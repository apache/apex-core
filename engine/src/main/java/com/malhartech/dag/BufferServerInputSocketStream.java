/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.dag;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.malhartech.bufferserver.ClientHandler;

public class BufferServerInputSocketStream extends InputSocketStream
{
  private static Logger LOG = LoggerFactory.getLogger(BufferServerInputSocketStream.class);

  /**
   * Requires upstream node info to setup subscriber TODO: revisit context
   */
  public void setContext(StreamContext context, String upstreamNodeId, String upstreamNodeLogicalName, String downstreamNodeId)
  {
    super.setContext(context);
    String downstreamNodeLogicalName = "downstreamNodeLogicalNotRequired"; // TODO: why do we need this?
    LOG.info("registering subscriber: id={} upstreamId={} upstreamLogical={}", new Object[] {downstreamNodeId, upstreamNodeId, upstreamNodeLogicalName});
    ClientHandler.registerPartitions(channel, downstreamNodeId, downstreamNodeLogicalName, upstreamNodeId, upstreamNodeLogicalName, Collections.<String>emptyList());
  }
  
  @Override
  public void setContext(StreamContext context) {
    throw new UnsupportedOperationException("setContext requires additional parameters.");
  }
  
}