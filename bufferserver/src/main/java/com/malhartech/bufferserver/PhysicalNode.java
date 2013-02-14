/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.malhartech.bufferserver.util.SerializedData;
import com.malhartech.bufferserver.util.WaitingChannelFutureListener;
import io.netty.channel.Channel;

/**
 *
 * @author chetan
 */
public class PhysicalNode
{
  public static final int BUFFER_SIZE = 64 * 1024;
  private int writtenBytes;
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

  final WaitingChannelFutureListener wcfl = new WaitingChannelFutureListener();
  /**
   *
   * @param d
   */
  public void send(SerializedData d)
  {
    if (BUFFER_SIZE - writtenBytes > d.size) {
      channel.write(d);
      writtenBytes += d.size;
    }
    else {
      synchronized (wcfl) {
        if (wcfl.isAdded()) {
          try {
            wcfl.wait();
            if (d.size < BUFFER_SIZE) {
              channel.write(d);
            }
            else {
              wcfl.setAdded(true);
              channel.write(d).addListener(wcfl);
            }
          }
          catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
        else {
          wcfl.setAdded(true);
          channel.write(d).addListener(wcfl);
        }

        writtenBytes = d.size;
      }
    }
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
