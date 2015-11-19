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

import java.security.AccessControlException;

import com.datatorrent.netlet.AbstractLengthPrependerClient;

/**
 * <p>Auth Client class.</p>
 *
 * @since 3.0.0
 */
public abstract class AuthClient extends AbstractLengthPrependerClient
{
  private byte[] token;

  public AuthClient()
  {
  }

  public AuthClient(int readBufferSize, int sendBufferSize)
  {
    super(readBufferSize, sendBufferSize);
  }

  public AuthClient(byte[] readbuffer, int position, int sendBufferSize)
  {
    super(readbuffer, position, sendBufferSize);
  }

  protected void sendAuthenticate()
  {
    if (token != null) {
      write(token);
    }
  }

  protected void authenticateMessage(byte[] buffer, int offset, int size)
  {
    if (token != null) {
      boolean authenticated = false;
      if (size == token.length) {
        int match = 0;
        while ((match < token.length) && (buffer[offset + match] == token[match])) {
          ++match;
        }
        if (match == token.length) {
          authenticated = true;
        }
      }
      if (!authenticated) {
        throw new AccessControlException("Buffer server security is enabled." +
            " Access is restricted without proper credentials.");
      }
    }
  }

  public void setToken(byte[] token)
  {
    this.token = token;
  }
}
