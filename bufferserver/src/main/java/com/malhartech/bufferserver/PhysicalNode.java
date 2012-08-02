/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.util.SerializedData;
import org.jboss.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public class PhysicalNode
{
  private final long starttime;
  private final Channel channel;
  private long processedMessageCount;

  public PhysicalNode(Channel channel)
  {
    this.channel = channel;
    starttime = System.currentTimeMillis();
    processedMessageCount = 0;
  }

  public long getstartTime()
  {
    return starttime;
  }

  public long getUptime()
  {
    return System.currentTimeMillis() - starttime;
  }

  public void send(SerializedData d)
  {
    getChannel().write(d);
    processedMessageCount++;
  }

  public final long getProcessedMessageCount()
  {
    return processedMessageCount;
  }

  @Override
  public boolean equals(Object o)
  {
    return o.hashCode() == this.hashCode();
  }

  public final int getId()
  {
    return getChannel().getId();
  }

  @Override
  public final int hashCode()
  {
    return getChannel().getId();
  }

  /**
   * @return the channel
   */
  public Channel getChannel()
  {
    return channel;
  }
}
