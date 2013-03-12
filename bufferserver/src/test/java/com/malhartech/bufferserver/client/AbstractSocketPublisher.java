/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import java.net.InetSocketAddress;
import java.util.Arrays;
import malhar.netlet.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class AbstractSocketPublisher extends Client
{
  public EventLoop eventloop;
  InetSocketAddress address;


  public void setup(String host, int port)
  {
    if (host == null) {
      address = new InetSocketAddress(port);
    }
    else{
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

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    logger.warn("received data when unexpected ", Arrays.toString(Arrays.copyOfRange(buffer, offset, size)));
  }

  private static final Logger logger = LoggerFactory.getLogger(AbstractSocketPublisher.class);
}