/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.PublishRequestTuple;
import java.io.IOException;
import malhar.netlet.DefaultEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BufferServerPublisher extends AbstractSocketPublisher
{
  private final String id;
  public int baseWindow;
  public int windowId;

  public BufferServerPublisher(String id)
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

  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
}
