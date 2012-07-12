/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import com.malhartech.bufferserver.ClientHandler;
import com.malhartech.dag.StreamContext;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferServerInputSocketStream extends InputSocketStream
{
  private static Logger LOG = LoggerFactory.getLogger(BufferServerInputSocketStream.class);

  /**
   * Requires upstream node info to setup subscriber TODO: revisit context
   */
  public void setContext(StreamContext context, String upstreamNodeId, String streamLogicalName, String downstreamNodeId)
  {
    super.setContext(context);
    String type = "paramNotRequired?"; // TODO: why do we need this?
    LOG.info("registering subscriber: id={} upstreamId={} streamLogicalName={}", new Object[] {downstreamNodeId, upstreamNodeId, streamLogicalName});
    ClientHandler.registerPartitions(channel, downstreamNodeId, streamLogicalName, upstreamNodeId, type, Collections.<String>emptyList());
  }
  
  @Override
  public void setContext(StreamContext context) {
    throw new UnsupportedOperationException("setContext requires additional parameters.");
  }
  
}