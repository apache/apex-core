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
package com.datatorrent.bufferserver.client;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datatorrent.bufferserver.packet.SubscribeRequestTuple.getSerializedRequest;

/**
 *
 */
/**
 * Implement tuple flow from buffer server to the node in a logical stream<p>
 * <br>
 * Extends SocketInputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a read instance of a stream and takes care of connectivity with upstream buffer server<br>
 *
 * @since 0.3.2
 */
public abstract class Subscriber extends AuthClient
{
  private final String id;

  public Subscriber(String id)
  {
    super(64 * 1024, 1024);
    this.id = id;
  }

  public void activate(final String version, final String type, final String sourceId, final int mask,
      final Collection<Integer> partitions, final long windowId, final int bufferSize)
  {
    sendAuthenticate();
    write(getSerializedRequest(version, id, type, sourceId, mask, partitions, windowId, bufferSize));
  }

  @Override
  public String toString()
  {
    return "Subscriber{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Subscriber.class);
}
