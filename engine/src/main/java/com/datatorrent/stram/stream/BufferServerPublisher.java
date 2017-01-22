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
import com.datatorrent.bufferserver.client.Publisher;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.EventLoop;
import com.datatorrent.stram.engine.StreamContext;

/**
 * Implements tuple flow of node to then buffer server in a logical stream<p>
 * <br>
 * Extends SocketOutputStream as buffer server and node communicate via a socket<br>
 * This buffer server is a write instance of a stream and hence would take care of persistence and retaining tuples till they are consumed<br>
 * Partitioning is managed by this instance of the buffer server<br>
 * <br>
 *
 * @since 0.3.2
 */
public class BufferServerPublisher extends AbstractPublisher
{
  private EventLoop eventloop;
  Publisher publisher;

  public BufferServerPublisher(String sourceId, int queueCapacity)
  {
    super();
    publisher = new Publisher(sourceId, queueCapacity)
    {
      @Override
      public void onMessage(byte[] buffer, int offset, int size)
      {
        throw new RuntimeException("OutputStream is not supposed to receive anything!");
      }
    };
  }

  @Override
  public void send(byte[] data)
  {
    try {
      while (!publisher.write(data)) {
        java.lang.Thread.sleep(5);
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  /**
   *
   * @param context
   */
  @Override
  @SuppressWarnings("unchecked")
  public void activate(StreamContext context)
  {
    publisher.setToken(context.get(StreamContext.BUFFER_SERVER_TOKEN));
    InetSocketAddress address = context.getBufferServerAddress();
    eventloop = context.get(StreamContext.EVENT_LOOP);
    eventloop.connect(address.isUnresolved() ? new InetSocketAddress(address.getHostName(), address.getPort()) : address, publisher);

    logger.debug("Registering publisher: {} {} windowId={} server={}", new Object[] {context.getSourceId(), context.getId(), Codec.getStringWindowId(context.getFinishedWindowId()), context.getBufferServerAddress()});
    publisher.activate(null, context.getFinishedWindowId());
  }

  @Override
  public void deactivate()
  {
    publisher.setToken(null);
    eventloop.disconnect(publisher);
  }

  private static final Logger logger = LoggerFactory.getLogger(BufferServerPublisher.class);
}
