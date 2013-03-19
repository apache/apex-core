/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.bufferserver.client.ClientHandler;
import com.malhartech.bufferserver.client.VarIntLengthPrependerClient;
import com.malhartech.bufferserver.packet.PurgeRequestTuple;
import com.malhartech.bufferserver.packet.ResetRequestTuple;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates buffer server control interface, used by the master for purging data.
 */
class BufferServerClient extends VarIntLengthPrependerClient
{
  /**
   * Use a single thread group for all buffer server interactions.
   */
  final InetSocketAddress addr;

  BufferServerClient(InetSocketAddress addr)
  {
    this.addr = new InetSocketAddress(addr.getHostName(), addr.getPort());
  }

  void purge(String sourceIdentifier, long windowId)
  {
    StramChild.eventloop.connect(this.addr, this);

    logger.debug("Purging sourceId=" + sourceIdentifier + ", windowId=" + windowId + " @" + addr);
    write(PurgeRequestTuple.getSerializedRequest(sourceIdentifier, windowId));
  }

  void reset(String sourceIdentifier, long windowId)
  {
    StramChild.eventloop.connect(this.addr, this);

    logger.debug("Reset sourceId=" + sourceIdentifier + ", windowId=" + windowId + " @" + addr);
    write(ResetRequestTuple.getSerializedRequest(sourceIdentifier, windowId));
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    StramChild.eventloop.disconnect(this);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerClient.class);
}
