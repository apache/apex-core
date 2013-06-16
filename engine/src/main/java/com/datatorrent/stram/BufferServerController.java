/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stram;

import com.malhartech.bufferserver.client.Controller;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates buffer server control interface, used by the master for purging data.
 */
class BufferServerController extends Controller
{
  /**
   * Use a single thread group for all buffer server interactions.
   */
  InetSocketAddress addr;

  BufferServerController(String id)
  {
    super(id);
  }

  @Override
  public void onMessage(String message)
  {
    logger.debug("Controller received {}, now disconnecting.", message);
    StramChild.eventloop.disconnect(this);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerController.class);
}
