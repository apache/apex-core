/**
 * Copyright (c) 2012-2013 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.stram;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.client.Controller;

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
