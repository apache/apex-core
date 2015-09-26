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
package com.datatorrent.stram.stream;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.stram.engine.StreamContext;

/**
 * <p>FastSubscriber class.</p>
 *
 * TODO:- Implement token security
 *
 * @since 0.3.2
 */
public class FastSubscriber extends BufferServerSubscriber
{
  public FastSubscriber(String id, int queueCapacity)
  {
    super(id, queueCapacity);
  }

  @Override
  public void activate(StreamContext context)
  {
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, this);

    logger.debug("registering subscriber: id={} upstreamId={} streamLogicalName={} windowId={} mask={} partitions={} server={}", new Object[] {context.getSinkId(), context.getSourceId(), context.getId(), context.getFinishedWindowId(), context.getPartitionMask(), context.getPartitions(), context.getBufferServerAddress()});
    activate(Tuple.FAST_VERSION, context.getId() + '/' + context.getSinkId(), context.getSourceId(), context.getPartitionMask(), context.getPartitions(), context.getFinishedWindowId(), freeFragments.capacity());
  }

  @Override
  public int readSize()
  {
    if (writeOffset - readOffset < 2) {
      return -1;
    }

    short s = buffer[readOffset++];
    return s | (buffer[readOffset++] << 8);
  }

  private static final Logger logger = LoggerFactory.getLogger(FastSubscriber.class);
}
