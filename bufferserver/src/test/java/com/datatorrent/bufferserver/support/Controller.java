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

/**
 *
 */
public class Controller extends com.datatorrent.bufferserver.client.Controller
{
  public String data;

  public Controller(String id)
  {
    super(id);
  }

  @Override
  public void purge(String version, String sourceId, long windowId)
  {
    data = null;
    super.purge(version, sourceId, windowId);
  }

  @Override
  public void reset(String version, String sourceId, long windowId)
  {
    data = null;
    super.reset(version, sourceId, windowId);
  }

  @Override
  public void onMessage(String message)
  {
    data = message;
  }

}
