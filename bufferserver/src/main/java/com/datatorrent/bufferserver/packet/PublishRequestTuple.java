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
package com.datatorrent.bufferserver.packet;

/**
 * <p>PublishRequestTuple class.</p>
 *
 * @since 0.3.2
 */
public class PublishRequestTuple extends GenericRequestTuple
{
  public PublishRequestTuple(byte[] array, int offset, int len)
  {
    super(array, offset, len);
  }

  public MessageType getType()
  {
    return MessageType.PUBLISHER_REQUEST;
  }

  public static byte[] getSerializedRequest(final String version, final String identifier, final long startingWindowId)
  {
    return GenericRequestTuple.getSerializedRequest(version, identifier, startingWindowId,
        MessageType.PUBLISHER_REQUEST_VALUE);
  }

}
