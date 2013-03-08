/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.server;

import com.googlecode.connectlet.Connection;
import com.malhartech.bufferserver.util.SerializedData;
import java.nio.channels.SocketChannel;
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
  private final Connection connection;
  private long processedMessageCount;

  /**
   *
   * @param channel
   */
  public PhysicalNode(Connection channel)
  {
    this.connection = channel;
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
   * @return
   * @throws InterruptedException
   */
  public void send(SerializedData d) throws InterruptedException
  {
    connection.send(d.bytes, d.offset, d.size);
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
    return connection.hashCode();
  }

  /**
   *
   * @return int
   */
  @Override
  public final int hashCode()
  {
    return connection.hashCode();
  }

  /**
   * @return the channel
   */
  public Connection getConnection()
  {
    return connection;
  }

  private static final Logger logger = LoggerFactory.getLogger(PhysicalNode.class);
}
