/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.bufferserver.internal;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.util.SerializedData;
import com.datatorrent.netlet.WriteOnlyClient;

/**
 * PhysicalNode represents one physical subscriber.
 *
 * @since 0.3.2
 */
public class PhysicalNode
{
  private final long starttime;
  private final WriteOnlyClient client;
  private long processedMessageCount;
  private SerializedData blocker;

  /**
   *
   * @param client
   */
  public PhysicalNode(WriteOnlyClient client)
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
  public boolean send(SerializedData d)
  {
    if (client.send(d.buffer, d.dataOffset, d.length - (d.dataOffset - d.offset))) {
      return true;
    }
    if (blocker == null) {
      blocker = d;
    } else if (blocker != d) {
      throw new IllegalStateException(String.format("Can't send data %s while blocker %s is pending on %s", d, blocker, this));
    }
    return false;
  }

  public boolean unblock()
  {
    if (blocker == null) {
      return true;
    }

    if (client.send(blocker.buffer, blocker.dataOffset, blocker.length - (blocker.dataOffset - blocker.offset))) {
      blocker = null;
      return true;
    }

    return false;
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
  public WriteOnlyClient getClient()
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
