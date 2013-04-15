/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.client;

import com.malhartech.bufferserver.packet.PublishRequestTuple;
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

  /**
   *
   * @param windowId
   */
  public void activate(long windowId)
  {
    write(PublishRequestTuple.getSerializedRequest(id, windowId));
  }

  @Override
  public String toString()
  {
    return "Publisher{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
