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

import com.datatorrent.netlet.util.VarInt;

/**
 * <p>ResetWindowTuple class.</p>
 *
 * @since 0.3.2
 */
public class ResetWindowTuple extends Tuple
{
  private final int baseSeconds;
  private final int windowWidth;

  public ResetWindowTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
    baseSeconds = readVarInt();
    windowWidth = readVarInt();
  }

  @Override
  public MessageType getType()
  {
    return MessageType.RESET_WINDOW;
  }

  @Override
  public int getBaseSeconds()
  {
    return baseSeconds;
  }

  @Override
  public int getWindowWidth()
  {
    return windowWidth;
  }

  public static byte[] getSerializedTuple(int baseSeconds, int windowWidth)
  {
    int size = 1; /* for type */

    /* for baseSeconds */
    int bits = 32 - Integer.numberOfLeadingZeros(baseSeconds);
    size += bits / 7 + 1;

    /* for windowWidth */
    bits = 32 - Integer.numberOfLeadingZeros(windowWidth);
    size += bits / 7 + 1;

    byte[] buffer = new byte[size];
    size = 0;

    buffer[size++] = MessageType.RESET_WINDOW_VALUE;
    size = VarInt.write(baseSeconds, buffer, size);
    VarInt.write(windowWidth, buffer, size);

    return buffer;
  }

}
