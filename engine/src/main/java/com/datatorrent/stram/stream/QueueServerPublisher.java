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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.bufferserver.server.Server;
import com.datatorrent.stram.engine.StreamContext;

public class QueueServerPublisher extends AbstractPublisher
{
  private Server.QueuePublisher queuePublisher;
  private Server server;
  private String identifier;
  private int queueCapacity;

  public QueueServerPublisher(String sourceId, Server server, int queueCapacity)
  {
    super();
    this.server = server;
    this.identifier = sourceId;
    this.queueCapacity = queueCapacity;
  }

  @Override
  public void activate(StreamContext context)
  {
    queuePublisher = server.handleQueuePublisher(context.getFinishedWindowId(), identifier, queueCapacity);
  }

  @Override
  public void send(byte[] data)
  {
    queuePublisher.send(data);
  }

  private static final Logger logger = LoggerFactory.getLogger(QueueServerPublisher.class);
}
