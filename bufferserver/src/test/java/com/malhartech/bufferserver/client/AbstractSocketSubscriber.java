/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import java.net.InetSocketAddress;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @param <T>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public abstract class AbstractSocketSubscriber<T> extends ProtoBufClient
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractSocketSubscriber.class);
  InetSocketAddress address;
  public DefaultEventLoop eventloop;

  public void setup(String host, int port)
  {
    if (host == null) {
      address = new InetSocketAddress(port);
    }
    else {
      address = new InetSocketAddress(host, port);
    }
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

}
