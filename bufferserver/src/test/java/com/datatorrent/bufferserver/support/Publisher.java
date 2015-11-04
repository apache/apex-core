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
package com.datatorrent.bufferserver.support;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Publisher extends com.datatorrent.bufferserver.client.Publisher
{
  public Publisher(String id)
  {
    super(id);
  }

  @Override
  public String toString()
  {
    return "BufferServerPublisher";
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    logger.warn("received data when unexpected {}", Arrays.toString(Arrays.copyOfRange(buffer, offset, size)));
  }

  public void publishMessage(byte[] payload)
  {
    write(payload);
  }

  public void activate(String version, int baseSeconds, int windowId)
  {
    super.activate(version, (long)baseSeconds << 32 | windowId);
  }

  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
}
