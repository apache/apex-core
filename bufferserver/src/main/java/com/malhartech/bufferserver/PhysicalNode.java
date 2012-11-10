/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.util.SerializedData;
import io.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public class PhysicalNode
{
  private final long starttime;
  private final Channel channel;
  private long processedMessageCount;

  /**
   *
   * @param channel
   */
  public PhysicalNode(Channel channel)
  {
    this.channel = channel;
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
   */
  public void send(SerializedData d)
  {
    getChannel().write(d);
    processedMessageCount++;
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
    return getChannel().id();
  }

  /**
   *
   * @return int
   */
  @Override
  public final int hashCode()
  {
    return getChannel().id();
  }

  /**
   * @return the channel
   */
  public Channel getChannel()
  {
    return channel;
  }
}
