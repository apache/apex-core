/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.PublishRequestTuple;
import java.io.IOException;
import java.util.Arrays;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Publisher extends AbstractClient
{
  private final String id;
  public int baseWindow;
  public int windowId;

  public Publisher(String id)
  {
    this.id = id;
  }

  public void publishMessage(byte[] payload)
  {
    write(payload);
  }

  /**
   *
   */
  @Override
  public void activate()
  {
    super.activate();
    write(PublishRequestTuple.getSerializedRequest(id, (long)baseWindow << 32 | windowId));
  }

  @Override
  public void handleException(Exception cce, DefaultEventLoop el)
  {
    if (cce instanceof IOException) {
      el.disconnect(this);
    }
    else {
      throw new RuntimeException(cce);
    }
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

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
