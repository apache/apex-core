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
public abstract class Publisher extends AbstractClient
{
  private final String id;

  public Publisher(String id)
  {
    super(1024, 16 * 1024);
    this.id = id;
  }

  @Override
  public void activate()
  {
    throw new RuntimeException("please use 'void activate(long windowId)' instead");
  }
  /**
   *
   * @param windowId
   */
  public void activate(long windowId)
  {
    super.activate();
    write(PublishRequestTuple.getSerializedRequest(id, windowId));
  }

  @Override
  public String toString()
  {
    return "Publisher{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
