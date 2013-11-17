/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.client;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.PublishRequestTuple;
import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 * <p>Abstract Publisher class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public abstract class Publisher extends AbstractLengthPrependerClient
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
