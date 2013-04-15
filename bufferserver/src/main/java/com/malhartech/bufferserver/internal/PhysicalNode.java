/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.internal;

import com.malhartech.bufferserver.client.AbstractClient;
import com.malhartech.bufferserver.util.SerializedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PhysicalNode represents one physical subscriber.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class PhysicalNode
{
  public static final int BUFFER_SIZE = 8 * 1024;
  private final long starttime;
  private final AbstractClient client;
  private long processedMessageCount;

  /**
   *
   * @param client
   */
  public PhysicalNode(AbstractClient client)
  {
    this.client = client;
    starttime = System.currentTimeMillis();
    processedMessageCount = 0;
  }

  /**
   *
   * @return long
   */
  public long getstartTime()
  {
    return starttime;
  }

  /**
   *
   * @return long
   */
  public long getUptime()
  {
    return System.currentTimeMillis() - starttime;
  }

  /**
   *
   * @param d
   * @throws InterruptedException
   */
  private SerializedData blocker;

  public boolean send(SerializedData d)
  {
    if (d.offset == d.dataOffset) {
      if (client.write(d.bytes, d.offset, d.size)) {
        return true;
      }
    }
    else {
      if (client.send(d.bytes, d.offset, d.size)) {
        return true;
      }
    }

    blocker = d;
    return false;
  }

  public boolean unblock()
  {
    if (blocker == null) {
      return true;
    }

    if (send(blocker)) {
      blocker = null;
      return true;
    }

    return false;
  }

  public boolean isBlocked()
  {
    return blocker != null;
  }

  /**
   *
   * @return long
   */
  public final long getProcessedMessageCount()
  {
    return processedMessageCount;
  }

  /**
   *
   * @param o
   * @return boolean
   */
  @Override
  public boolean equals(Object o)
  {
    return o == this || (o.getClass() == this.getClass() && o.hashCode() == this.hashCode());
  }

  /**
   *
   * @return int
   */
  public final int getId()
  {
    return client.hashCode();
  }

  /**
   *
   * @return int
   */
  @Override
  public final int hashCode()
  {
    return client.hashCode();
  }

  /**
   * @return the channel
   */
  public AbstractClient getClient()
  {
    return client;
  }

  @Override
  public String toString()
  {
    return "PhysicalNode." + client;
  }

  private static final Logger logger = LoggerFactory.getLogger(PhysicalNode.class);
}
