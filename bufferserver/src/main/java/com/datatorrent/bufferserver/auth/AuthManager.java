/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.bufferserver.auth;

import java.security.SecureRandom;

/**
 * <p>Auth Manager class.</p>
 */
public class AuthManager
{
  private final static int BUFFER_SERVER_TOKEN_LENGTH = 20;

  private static SecureRandom generator = new SecureRandom();

  public static byte[] generateToken()
  {
    byte[] token = new byte[BUFFER_SERVER_TOKEN_LENGTH];
    generator.nextBytes(token);
    return token;
  }
}
