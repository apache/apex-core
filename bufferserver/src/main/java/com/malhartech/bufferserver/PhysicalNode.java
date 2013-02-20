/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.util.SerializedData;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureAggregator;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author chetan
 */
public class PhysicalNode
{
  public static final int BUFFER_SIZE = 512 * 1024;
  private volatile int writtenBytes;
  private final long starttime;
  private final Channel channel;
  private long processedMessageCount;
  private final ChannelFutureListener cfl = new ChannelFutureListener()
  {
    public void operationComplete(ChannelFuture future) throws Exception
    {
      writtenBytes = 0;
    }

  };

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
   * @throws InterruptedException
   */
  public boolean send(SerializedData d) throws InterruptedException
  {
    channel.write(d);
    writtenBytes += d.size;
    processedMessageCount++;
    if (writtenBytes > BUFFER_SIZE) {
      channel.flush().addListener(cfl);
      return true;
    }

    return false;
  }

  public boolean isBlocked()
  {
    return writtenBytes > BUFFER_SIZE;
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

  private static final Logger logger = LoggerFactory.getLogger(PhysicalNode.class);
}
