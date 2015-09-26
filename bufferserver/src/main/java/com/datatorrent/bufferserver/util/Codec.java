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
package com.datatorrent.bufferserver.util;

/**
 * <p>Codec class.</p>
 *
 * @since 0.9.1
 */
public class Codec
{
  public static final long INVALID_WINDOW_ID = -1;
  public static final String UNPARSEABLE_WINDOW_ID = Long.toHexString(INVALID_WINDOW_ID);
  public static String getStringWindowId(long windowId)
  {
    if (windowId == INVALID_WINDOW_ID) {
      return UNPARSEABLE_WINDOW_ID;
    }

    return Long.toHexString(windowId);
  }

  public static long getLongWindowId(String windowId)
  {
    if (UNPARSEABLE_WINDOW_ID.equals(windowId)) {
      return INVALID_WINDOW_ID;
    }

    return Long.parseLong(windowId, 16);
  }
}
