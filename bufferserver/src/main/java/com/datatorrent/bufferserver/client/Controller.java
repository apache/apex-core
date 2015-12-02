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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.bufferserver.packet.MessageType;
import com.datatorrent.bufferserver.packet.PurgeRequestTuple;
import com.datatorrent.bufferserver.packet.ResetRequestTuple;
import com.datatorrent.bufferserver.packet.Tuple;
import com.datatorrent.bufferserver.util.Codec;
import com.datatorrent.netlet.util.Slice;

/**
 * <p>Abstract Controller class.</p>
 *
 * @since 0.3.2
 */
public abstract class Controller extends AuthClient
{
  String id;

  public Controller(String id)
  {
    super(1024, 1024);
    this.id = id;
  }

  public void purge(String version, String sourceId, long windowId)
  {
    sendAuthenticate();
    write(PurgeRequestTuple.getSerializedRequest(version, sourceId, windowId));
    logger.debug("Sent purge request sourceId = {}, windowId = {}", sourceId, Codec.getStringWindowId(windowId));
  }

  public void reset(String version, String sourceId, long windowId)
  {
    sendAuthenticate();
    write(ResetRequestTuple.getSerializedRequest(version, sourceId, windowId));
    logger.debug("Sent reset request sourceId = {}, windowId = {}", sourceId, Codec.getStringWindowId(windowId));
  }

  @Override
  public void onMessage(byte[] buffer, int offset, int size)
  {
    Tuple t = Tuple.getTuple(buffer, offset, size);
    assert (t.getType() == MessageType.PAYLOAD);
    Slice f = t.getData();
    onMessage(new String(f.buffer, f.offset, f.length));
  }

  public abstract void onMessage(String message);

  @Override
  public String toString()
  {
    return "Controller{" + "id=" + id + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(Controller.class);
}
