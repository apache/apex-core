/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.support;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class Publisher extends com.datatorrent.bufferserver.client.Publisher
{
  public Publisher(String id)
  {
    super(id);
  }

  @Override
  public String toString()
  {
    return "BufferServerPublisher";
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    logger.warn("received data when unexpected {}", Arrays.toString(Arrays.copyOfRange(buffer, offset, size)));
  }

  public void publishMessage(byte[] payload)
  {
    write(payload);
  }

  public void activate(String version, int baseSeconds, int windowId)
  {
    super.activate(version, (long)baseSeconds << 32 | windowId);
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
