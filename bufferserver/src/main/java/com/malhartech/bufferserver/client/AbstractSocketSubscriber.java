/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import java.net.InetSocketAddress;
import malhar.netlet.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractSocketSubscriber extends VarIntLengthPrependerClient
{
  private InetSocketAddress address;
  private EventLoop eventloop;

  public void setup(InetSocketAddress address, EventLoop eventloop)
  {
    this.address = address;
    this.eventloop = eventloop;
  }

  public void teardown()
  {
  }

  public void activate()
  {
    eventloop.connect(address, this);
  }

  public void deactivate()
  {
    eventloop.disconnect(this);
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractSocketSubscriber.class);
}
