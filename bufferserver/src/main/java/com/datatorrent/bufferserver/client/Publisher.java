/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.client;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public abstract class Publisher extends AbstractClient
{
  private final String id;

  public Publisher(String id)
  {
    this(id, 1024);
  }

  public Publisher(String id, int sendBufferCapacity)
  {
    super(1024, sendBufferCapacity);
    this.id = id;
  }

  /**
   *
   * @param windowId
   */
  public void activate(String version, long windowId)
  {
    write(PublishRequestTuple.getSerializedRequest(version, id, windowId));
  }

  @Override
  public String toString()
  {
    return "Publisher{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
